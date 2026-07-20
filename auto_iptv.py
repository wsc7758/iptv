import os
import re
import sys
import time
import traceback
from collections import defaultdict, OrderedDict
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError
import warnings
import requests

# ====================== 全局常量（统一管理，无冗余） ======================
BASE_PATH = "config/"
SOURCES_FILE = os.path.join(BASE_PATH, "sources.txt")
ALIAS_FILE = os.path.join(BASE_PATH, "alias.txt")
ALLOW_LIST_FILE = os.path.join(BASE_PATH, "allow_list.txt")
TEMPLATE_OUTPUT_FILE = os.path.join(BASE_PATH, "template_output.txt")
BLACKLIST_FILE = os.path.join(BASE_PATH, "blacklist.txt")
LOW_LINK_CHANNEL_FILE = os.path.join(BASE_PATH, "low_link_channel.txt")
OLD_IPTV_FILE = "iptv.txt"
OUTPUT_TXT = "iptv.txt"
TV_BOX_OUTPUT = "tv.txt"

# 测速参数（沿用你原有配置，无改动）
STREAM_CONNECT_TIMEOUT = 0.8
STREAM_REQ_TIMEOUT = 1.5
STREAM_WORKERS = 2
MIN_STOCK_LINK = 4
BATCH_SIZE = 4
BATCH_TIMEOUT = 18
RETRY_BATCH_SIZE = 2
RETRY_BATCH_TIMEOUT = 20
MAX_RETRY_POOL = 200
LATENCY_THRESHOLD = 1.2
MAX_CHANNEL_LINK = 8
BAD_DOMAIN = {"bfgd", "txiptv", "3000", "tvtv", "live4k"}

# 环境初始化
warnings.filterwarnings("ignore")
requests.packages.urllib3.disable_warnings()

# ====================== 通用工具函数 ======================
def print_log(msg: str):
    """统一日志打印，全局只写一次flush"""
    print(msg, flush=True)

def clean_url(url: str) -> str:
    """清洗URL后缀符号"""
    return re.sub(r"\$.*$", "", url.strip())

def is_black_url(url: str, black_list: list) -> bool:
    """URL黑名单校验+垃圾域名前置校验合并"""
    for d in BAD_DOMAIN:
        if d in url:
            return True
    for kw in black_list:
        if kw in url:
            return True
    return False

def batch_run_tasks(task_list, task_func, url_black, batch_size, batch_timeout, workers, total_all):
    """统一测速/重试执行器，一套逻辑复用，消除重复代码
    :param total_all: 总任务总数，解决跨函数变量作用域问题
    """
    finish_count = 0
    task_result = defaultdict(list)
    timeout_remain = []

    total = len(task_list)
    for batch_idx, start in enumerate(range(0, total, batch_size), 1):
        batch = task_list[start:start + batch_size]
        end = min(start + batch_size, total)
        print_log(f"\n===== 第{batch_idx}批测速：{start+1} ~ {end} / {total_all} =====")

        with ThreadPoolExecutor(max_workers=workers) as pool:
            future_map = {}
            task_store = {}
            for ch, link in batch:
                f = pool.submit(task_func, link, url_black)
                future_map[f] = ch
                task_store[f] = (ch, link)  # 绑定完整任务，不再读取私有属性
            try:
                for fut in as_completed(future_map, timeout=batch_timeout):
                    ch_name = future_map[fut]
                    url = task_store[fut][1]
                    try:
                        latency, link_ok, hit_black = fut.result(timeout=1)
                    except Exception:
                        latency = STREAM_REQ_TIMEOUT
                        link_ok = False
                        hit_black = False
                    finish_count += 1
                    if hit_black or finish_count % 50 == 0:
                        print_log(f"【总测速进度 {finish_count}/{total_all}】")
                    if hit_black:
                        continue
                    task_result[ch_name].append((latency, url, link_ok))
            except TimeoutError:
                print_log(f"⚠️ 第{batch_idx}批超过限时，缓存未完成链接待重试")
                # 安全读取未完成任务，不访问私有属性 _args
                unfinished = []
                for f in future_map:
                    if not f.done():
                        unfinished.append(task_store[f])
                add_num = min(len(unfinished), MAX_RETRY_POOL - len(timeout_remain))
                timeout_remain.extend(unfinished[:add_num])
                if add_num < len(unfinished):
                    print_log(f"⚠️ 重试池已满{MAX_RETRY_POOL}，丢弃{len(unfinished)-add_num}条链接")
    return task_result, timeout_remain, finish_count

# ====================== 业务独立函数 ======================
def load_black_list():
    exact_channel = set()
    fuzzy_key = []
    url_black = []
    if os.path.exists(BLACKLIST_FILE):
        with open(BLACKLIST_FILE, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if line.startswith("url*"):
                    url_black.append(line[4:].strip())
                elif line.startswith("*"):
                    fuzzy_key.append(line[1:].strip())
                else:
                    exact_channel.add(line)
    print_log(f"【黑名单加载】频道黑名单:{len(exact_channel)} 广告URL:{len(url_black)}")
    return exact_channel, fuzzy_key, url_black

def load_stock_channels(url_black):
    stock_map = defaultdict(list)
    filter_black = 0
    if not os.path.exists(OLD_IPTV_FILE):
        print_log(f"【提示】存量文件{OLD_IPTV_FILE}不存在")
        return stock_map, set(), filter_black
    with open(OLD_IPTV_FILE, "r", encoding="utf-8") as f:
        curr_ch = ""
        for line in f.readlines():
            line = line.strip()
            if line.startswith("#EXTINF") and "," in line:
                curr_ch = line.split(",")[-1].strip()
            elif line.startswith("http") and curr_ch:
                u = clean_url(line)
                if is_black_url(u, url_black):
                    filter_black += 1
                    continue
                stock_map[curr_ch].append((999.9, u, False))
    all_channel_set = set(stock_map.keys())
    print_log(f"【存量加载】频道总数{len(stock_map)}，过滤广告{filter_black}条")
    return stock_map, all_channel_set, filter_black

def build_need_test_list(stock_map, template_channel_set, all_stock_set):
    need_test = set()
    # 全新频道（存量不存在）
    new_ch = template_channel_set - all_stock_set
    need_test.update(new_ch)
    # 存量不足MIN_STOCK_LINK的频道
    for ch, link_list in stock_map.items():
        if len(link_list) < MIN_STOCK_LINK and ch in template_channel_set:
            need_test.add(ch)
    # 写入临时白名单
    with open(LOW_LINK_CHANNEL_FILE, "w", encoding="utf-8") as f:
        for ch in sorted(need_test):
            f.write(f"{ch}\n")
    print_log(f"【待测速频道生成】共{len(need_test)}个，存量≥{MIN_STOCK_LINK}直接跳过测速")
    return sorted(need_test)

def load_template():
    ch_map = {}
    ch_list = []
    ch_set = set()
    if not os.path.exists(TEMPLATE_OUTPUT_FILE):
        print_log(f"【致命错误】模板文件{TEMPLATE_OUTPUT_FILE}缺失，退出")
        sys.exit(1)
    with open(TEMPLATE_OUTPUT_FILE, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or "#genre#" in line:
                continue
            parts = line.split("|")
            if len(parts) >= 3:
                name, disp, group = parts[0].strip(), parts[1].strip(), parts[2].strip()
            elif len(parts) == 2:
                name, disp = parts[0].strip(), parts[1].strip()
                group = "默认分组"
            else:
                name = disp = line.strip()
                group = "默认分组"
            if name not in ch_set:
                ch_set.add(name)
                ch_list.append(name)
                ch_map[name] = (disp, group)
    print_log(f"【模板加载】频道总量{len(ch_list)}")
    return ch_list, ch_map, ch_set

def load_alias():
    alias = {}
    if os.path.exists(ALIAS_FILE):
        with open(ALIAS_FILE, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                parts = [p.strip() for p in line.split(",")]
                main = parts[0]
                for alt in parts:
                    alias[alt] = main
    print_log(f"【别名映射加载】共{len(alias)}组")
    return alias

def fetch_source(url):
    try:
        resp = requests.get(url, timeout=8, verify=False)
        resp.encoding = resp.apparent_encoding
        return resp.text
    except Exception as e:
        print_log(f"【警告】源{url}拉取失败：{str(e)[:60]}")
        return ""

def parse_m3u_or_txt(content: str):
    ch_link_map = defaultdict(list)
    if "#EXTM3U" in content:
        name = ""
        for line in content.splitlines():
            line = line.strip()
            if line.startswith("#EXTINF") and "," in line:
                name = line.split(",")[-1].strip()
            elif line.startswith("http"):
                u = clean_url(line)
                ch_link_map[name].append(u)
    else:
        for line in content.splitlines():
            line = line.strip().split("#")[0].strip()
            if "," not in line:
                continue
            ch, u = line.split(",", 1)
            ch = ch.strip()
            u = clean_url(u.strip())
            if u.startswith("http"):
                ch_link_map[ch].append(u)
    return ch_link_map

def test_single_link(url: str, url_black: list):
    """单条链接测速核心，全局只写一次"""
    start = time.perf_counter()
    if is_black_url(url, url_black):
        return (time.perf_counter() - start, False, True)
    ok = False
    try:
        resp = requests.get(
            url,
            timeout=(STREAM_CONNECT_TIMEOUT, STREAM_REQ_TIMEOUT),
            allow_redirects=True,
            verify=False,
            headers={"Range": "bytes=0-1023"}
        )
        if 200 <= resp.status_code < 400:
            ok = True
    except Exception:
        pass
    return (time.perf_counter() - start, ok, False)

def generate_m3u_tvbox(final_channel_data):
    m3u_lines = ["#EXTM3U x-tvg-url=\"epg.xml.gz\""]
    tvbox_group = OrderedDict()
    for ch_name, (disp_name, group, link_list) in final_channel_data.items():
        for lat, url, ok in link_list:
            m3u_lines.append(f'#EXTINF:-1 group-title="{group}",{disp_name}')
            m3u_lines.append(url)
            if group not in tvbox_group:
                tvbox_group[group] = []
            tvbox_group[group].append(f"{disp_name},{url}")
    # 生成TVBOX格式
    tvbox_lines = []
    for g, chs in tvbox_group.items():
        tvbox_lines.append(f"{g},#genre#")
        tvbox_lines.extend(chs)
    m3u_text = "\n".join(m3u_lines)
    tvbox_text = "\n".join(tvbox_lines)
    # 一次性写入文件
    with open(OUTPUT_TXT, "w", encoding="utf-8") as f:
        f.write(m3u_text)
    with open(TV_BOX_OUTPUT, "w", encoding="utf-8") as wf:
        wf.write(tvbox_text)
    print_log(f"【输出完成】M3U:{OUTPUT_TXT} TVBOX:{TV_BOX_OUTPUT}")
    return m3u_text, tvbox_text

# ====================== 主流程（线性化，无反复循环） ======================
def main():
    print_log("========== IPTV分拣程序启动 ==========")
    # 1. 加载基础配置（黑白名单、存量、模板、别名）
    exact_black_ch, fuzzy_kw, url_black_list = load_black_list()
    stock_channel_map, all_stock_ch_set, stock_filter_black = load_stock_channels(url_black_list)
    template_ch_list, template_ch_info, template_ch_set = load_template()
    alias_map = load_alias()

    # 2. 计算需要测速的频道列表
    need_test_channel = build_need_test_list(stock_channel_map, template_ch_set, all_stock_ch_set)
    allow_set = set(need_test_channel)

    # 3. 拉取所有订阅源，合并所有URL（只做一次全局去重）
    source_file_list = []
    if os.path.exists(SOURCES_FILE):
        with open(SOURCES_FILE, "r", encoding="utf-8") as f:
            source_file_list = [l.strip() for l in f.readlines() if l.strip() and not l.startswith("#")]
    print_log(f"\n【开始拉取订阅源，共{len(source_file_list)}个】")
    raw_all_channel_link = defaultdict(list)
    source_stat = defaultdict(lambda: {"total":0, "good":0, "fallback":0, "black":0})
    for idx, src_url in enumerate(source_file_list, 1):
        print_log(f"\n【{idx}/{len(source_file_list)}】拉取源：{src_url}")
        content = fetch_source(src_url)
        ch_link = parse_m3u_or_txt(content)
        total_line = sum(len(v) for v in ch_link.values())
        source_stat[src_url]["total"] = total_line
        print_log(f"【源解析完成】频道{len(ch_link)} 原始线路{total_line}")
        for ch, link_list in ch_link.items():
            real_ch = alias_map.get(ch, ch)
            raw_all_channel_link[real_ch].extend(link_list)

    # 4. 频道预处理：黑名单剔除 + URL全局去重 + 前置过滤垃圾域名
    test_task_list = []
    seen_global_url = set()
    drop_black_ch = 0
    for ch_name, link_list in raw_all_channel_link.items():
        # 频道黑名单过滤
        ch_black_flag = False
        if ch_name in exact_black_ch:
            ch_black_flag = True
        for kw in fuzzy_kw:
            if kw in ch_name:
                ch_black_flag = True
                break
        if ch_black_flag:
            print_log(f"【频道黑名单剔除】{ch_name}")
            drop_black_ch += 1
            continue
        # 仅保留需要测速的频道
        if ch_name not in allow_set:
            continue
        # URL全局去重 + 前置过滤垃圾域名，不送入测速队列
        unique_links = list(dict.fromkeys(link_list))
        for u in unique_links:
            if is_black_url(u, url_black_list):
                source_stat[src_url]["black"] += 1
                continue
            if u not in seen_global_url:
                seen_global_url.add(u)
                test_task_list.append((ch_name, u))
    total_task_num = len(test_task_list)
    print_log(f"【频道预处理结束】黑名单丢弃{drop_black_ch}个，待测速独立URL总数：{total_task_num}")

    # 5. 分批测速（封装统一执行函数，无重复代码，传入总任务数解决作用域）
    test_result, retry_pool, finish_cnt = batch_run_tasks(
        task_list=test_task_list,
        task_func=test_single_link,
        url_black=url_black_list,
        batch_size=BATCH_SIZE,
        batch_timeout=BATCH_TIMEOUT,
        workers=STREAM_WORKERS,
        total_all=total_task_num
    )
    print_log("\n【常规分批测速完成】")

    # 6. 重试批次超时遗留链接（复用同一套执行函数）
    valid_channel_cnt = sum(1 for _, link_list in test_result.items() if any(ok for _, _, ok in link_list))
    if len(retry_pool) > 0:
        print_log(f"\n========== 重试超时链接，共{len(retry_pool)}条 ==========")
        retry_result, _, _ = batch_run_tasks(
            task_list=retry_pool,
            task_func=test_single_link,
            url_black=url_black_list,
            batch_size=RETRY_BATCH_SIZE,
            batch_timeout=RETRY_BATCH_TIMEOUT,
            workers=STREAM_WORKERS,
            total_all=total_task_num
        )
        # 合并重试结果
        for ch, link_info in retry_result.items():
            test_result[ch].extend(link_info)
    print_log("\n【全部测速（含重试）执行完成】")

    # 7. 合并存量+测速新链接，封顶MAX_CHANNEL_LINK条
    final_channel_out = {}
    for ch_name in template_ch_list:
        disp_name, group = template_ch_info[ch_name]
        stock_links = stock_channel_map.get(ch_name, [])
        stock_url_set = set(u for _, u, _ in stock_links)
        if ch_name not in need_test_channel:
            # 存量充足，直接复用存量
            merge_links = stock_links
        else:
            # 合并优质测速链接 + 存量链接，去重
            new_raw = test_result.get(ch_name, [])
            good_new = []
            fallback_new = []
            for lat, u, ok in new_raw:
                if not ok:
                    continue
                if u in stock_url_set:
                    continue
                if lat <= LATENCY_THRESHOLD:
                    good_new.append((lat, u, ok))
                else:
                    fallback_new.append((lat, u, ok))
            good_new.sort(key=lambda x: x[0])
            fallback_new.sort(key=lambda x: x[0])
            merge_links = good_new + fallback_new + stock_links
        # 全局去重封顶
        final_unique = []
        exist_u = set()
        for item in merge_links:
            _, u, _ = item
            if u not in exist_u:
                exist_u.add(u)
                final_unique.append(item)
        final_out = final_unique[:MAX_CHANNEL_LINK]
        if len(final_out) == 0:
            continue
        final_channel_out[ch_name] = (disp_name, group, final_out)
        print_log(f"【频道合并完成】[{disp_name}] 留存线路{len(final_out)}条（存量{len(stock_links)}）")

    # 8. 输出M3U与TVBOX文件
    generate_m3u_tvbox(final_channel_out)

    # 9. 全局统计输出
    print_log("\n===================== 订阅源链路统计总表 =====================")
    for src, stat in source_stat.items():
        print_log(f"\n源地址：{src}")
        print_log(f" 原始线路：{stat['total']} 过滤广告：{stat['black']}")
        print_log(f" 优质链路：{stat['good']} 兜底慢速：{stat['fallback']}")
        print_log(f" 有效合计：{stat['good'] + stat['fallback']}")
    print_log("==============================================================")
    print_log(f"\n========== 全局执行汇总 ==========")
    print_log(f"测速存在有效线路频道总数：{valid_channel_cnt}")
    print_log(f"模板成功输出频道总数：{len(final_channel_out)}")
    print_log(f"临时测速频道清单：{LOW_LINK_CHANNEL_FILE}")
    print_log(f"存量阶段过滤广告链接：{stock_filter_black}")
    print_log("==== IPTV分拣程序全部执行完毕 ====")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print_log("脚本致命异常：")
        print_log(traceback.format_exc())
        sys.exit(1)
