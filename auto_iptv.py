import os
import re
import sys
import time
import traceback
from collections import defaultdict, OrderedDict
from concurrent.futures import ThreadPoolExecutor
import warnings
import requests

# ====================== 全局常量 ======================
BASE_PATH = "config/"
SOURCES_FILE = os.path.join(BASE_PATH, "sources.txt")
ALIAS_FILE = os.path.join(BASE_PATH, "alias.txt")
TEMPLATE_OUTPUT_FILE = os.path.join(BASE_PATH, "template_output.txt")
BLACKLIST_FILE = os.path.join(BASE_PATH, "blacklist.txt")
LOW_LINK_CHANNEL_FILE = os.path.join(BASE_PATH, "low_link_channel.txt")
OLD_IPTV_FILE = "iptv.txt"
OUTPUT_TXT = "iptv.txt"
TV_BOX_OUTPUT = "tv.txt"

# 测速参数
STREAM_CONNECT_TIMEOUT = 0.8
STREAM_REQ_TIMEOUT = 1.5
STREAM_WORKERS = 2
MIN_STOCK_LINK = 4
BATCH_SIZE = 4
BATCH_MAX_COST = 18  # 单批总耗时上限，不再作为as_completed超时
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
    print(msg, flush=True)

def clean_url(url: str) -> str:
    return re.sub(r"\$.*$", "", url.strip())

def is_black_url(url: str, black_list: list) -> bool:
    for d in BAD_DOMAIN:
        if d in url:
            return True
    for kw in black_list:
        if kw in url:
            return True
    return False

def batch_run_tasks(task_list, task_func, url_black, batch_size, batch_max_cost, workers, total_all):
    """
    全新无阻塞批次执行器：彻底移除as_completed批量timeout，不遍历未完成future
    逻辑：逐条接收完成任务，计时超标直接终止本批，丢弃所有剩余任务，零阻塞收集
    """
    finish_count = 0
    task_result = defaultdict(list)
    total = len(task_list)

    for batch_idx, start in enumerate(range(0, total, batch_size), 1):
        batch = task_list[start:start + batch_size]
        end = min(start + batch_size, total)
        print_log(f"\n===== 第{batch_idx}批测速：{start+1} ~ {end} / {total_all} =====")

        batch_start_time = time.perf_counter()
        pool = ThreadPoolExecutor(max_workers=workers)
        task_store = {pool.submit(task_func, link, url_black): (ch, link) for ch, link in batch}
        completed = set()

        # 循环接收已完成任务，不设置整批超时，靠单条请求快速失败
        while len(completed) < len(task_store):
            # 每轮只等0.3s，持续刷新日志，无长时间阻塞
            try:
                fut = next((f for f in task_store if f.done() and f not in completed), None)
                if fut is None:
                    time.sleep(0.3)
                    # 检测本批总耗时是否超限
                    if time.perf_counter() - batch_start_time > batch_max_cost:
                        print_log(f"⚠️ 第{batch_idx}批总耗时超过{batch_max_cost}s上限，直接丢弃所有未完成链接，跳过残留任务")
                        break
                    continue
            except StopIteration:
                time.sleep(0.3)
                continue

            # 处理已完成任务
            completed.add(fut)
            ch_name, url = task_store[fut]
            try:
                latency, link_ok, hit_black = fut.result(timeout=0.5)
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

        # 直接销毁线程池，完全不遍历未完成future，无任何阻塞判断
        pool.shutdown(wait=False, cancel_futures=True)
    return task_result, [], finish_count

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
    new_ch = template_channel_set - all_stock_set
    need_test.update(new_ch)
    for ch, link_list in stock_map.items():
        if len(link_list) < MIN_STOCK_LINK and ch in template_channel_set:
            need_test.add(ch)
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
    latency = time.perf_counter() - start
    return (latency, ok, False)

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
    tvbox_lines = []
    for g, chs in tvbox_group.items():
        tvbox_lines.append(f"{g},#genre#")
        tvbox_lines.extend(chs)
    m3u_text = "\n".join(m3u_lines)
    tvbox_text = "\n".join(tvbox_lines)
    with open(OUTPUT_TXT, "w", encoding="utf-8") as f:
        f.write(m3u_text)
    with open(TV_BOX_OUTPUT, "w", encoding="utf-8") as wf:
        wf.write(tvbox_text)
    print_log(f"【输出完成】M3U:{OUTPUT_TXT} TVBOX:{TV_BOX_OUTPUT}")
    return m3u_text, tvbox_text

# ====================== 主流程 ======================
def main():
    print_log("========== IPTV分拣程序启动 ==========")
    exact_black_ch, fuzzy_kw, url_black_list = load_black_list()
    stock_channel_map, all_stock_ch_set, stock_filter_black = load_stock_channels(url_black_list)
    template_ch_list, template_ch_info, template_ch_set = load_template()
    alias_map = load_alias()

    need_test_channel = build_need_test_list(stock_channel_map, template_ch_set, all_stock_ch_set)
    allow_set = set(need_test_channel)

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

    test_task_list = []
    seen_global_url = set()
    drop_black_ch = 0
    for ch_name, link_list in raw_all_channel_link.items():
        ch_black_flag = ch_name in exact_black_ch
        for kw in fuzzy_kw:
            if kw in ch_name:
                ch_black_flag = True
                break
        if ch_black_flag:
            print_log(f"【频道黑名单剔除】{ch_name}")
            drop_black_ch += 1
            continue
        if ch_name not in allow_set:
            continue
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

    # 执行测速，无重试逻辑
    test_result, _, finish_cnt = batch_run_tasks(
        task_list=test_task_list,
        task_func=test_single_link,
        url_black=url_black_list,
        batch_size=BATCH_SIZE,
        batch_max_cost=BATCH_MAX_COST,
        workers=STREAM_WORKERS,
        total_all=total_task_num
    )
    print_log("\n【全部测速执行完成】超时链接直接丢弃，无重试环节")
    valid_channel_cnt = sum(1 for _, link_list in test_result.items() if any(ok for _, _, ok in link_list))

    final_channel_out = {}
    for ch_name in template_ch_list:
        disp_name, group = template_ch_info[ch_name]
        stock_links = stock_channel_map.get(ch_name, [])
        stock_url_set = set(u for _, u, _ in stock_links)
        if ch_name not in need_test_channel:
            merge_links = stock_links
        else:
            new_raw = test_result.get(ch_name, [])
            good_new = []
            fallback_new = []
            for lat, u, ok in new_raw:
                if not ok or u in stock_url_set:
                    continue
                if lat <= LATENCY_THRESHOLD:
                    good_new.append((lat, u, ok))
                else:
                    fallback_new.append((lat, u, ok))
            good_new.sort(key=lambda x: x[0])
            fallback_new.sort(key=lambda x: x[0])
            merge_links = good_new + fallback_new + stock_links
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

    generate_m3u_tvbox(final_channel_out)

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
