import os
import re
import time
from collections import defaultdict, OrderedDict
from concurrent.futures import ThreadPoolExecutor, as_completed

# ====================== 全局配置区（测速参数收紧优化） ======================
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

# 测速业务常量【提速优化，适配GitHub Actions】
STREAM_REQ_TIMEOUT = 1.2    # 缩短单条等待时长
STREAM_RETRY_TIMES = 0     # 取消重试，减少重复请求
STREAM_REQ_WORKERS = 10    # 拉满并发，并行测速
MAX_LINK_PER_CHANNEL = 8
FALLBACK_MAX_LINK = 5
LATENCY_THRESHOLD = 0.8
MIN_STOCK_LINK = 5
# ======================================================

import warnings
warnings.filterwarnings("ignore")
import requests
requests.packages.urllib3.disable_warnings

# ====================== URL清洗工具 ======================
def clean_url_strip_suffix(raw_url: str) -> str:
    clean = raw_url.strip()
    clean = re.sub(r"\$.*$", "", clean)
    return clean

# ====================== 读取旧iptv存量数据（同步过滤黑名单URL） ======================
def parse_old_iptv_stock(url_black_list):
    stock_map = defaultdict(list)
    full_stock_channel_set = set()
    filter_black_count = 0
    if not os.path.exists(OLD_IPTV_FILE):
        print(f"【提示】未找到历史文件 {OLD_IPTV_FILE}")
        return stock_map, full_stock_channel_set, filter_black_count

    with open(OLD_IPTV_FILE, "r", encoding="utf-8") as f:
        lines = f.readlines()
    curr_ch = ""
    for line in lines:
        line = line.strip()
        if line.startswith("#EXTINF") and "," in line:
            curr_ch = line.split(",")[-1].strip()
            full_stock_channel_set.add(curr_ch)
        elif line.startswith("http") and curr_ch:
            clean_u = clean_url_strip_suffix(line)
            # 过滤存量中命中黑名单的URL
            if is_black_url(clean_u, url_black_list):
                filter_black_count += 1
                continue
            stock_map[curr_ch].append((999.9, clean_u, False))
    print(f"【历史iptv解析完成】存量频道总数：{len(stock_map)}，存量过滤广告链接：{filter_black_count} 条")
    return stock_map, full_stock_channel_set, filter_black_count

# ====================== 读取模板，有序频道列表+频道信息 ======================
def load_template_info():
    template_order_list = []
    channel_info = dict()
    template_std_set = set()
    if not os.path.exists(TEMPLATE_OUTPUT_FILE):
        print(f"【致命错误】模板文件 {TEMPLATE_OUTPUT_FILE} 不存在，无法运行")
        exit(1)
    with open(TEMPLATE_OUTPUT_FILE, "r", encoding="utf-8") as f:
        for line in f:
            raw_line = line.strip()
            if not raw_line or "#genre#" in raw_line:
                continue
            parts = raw_line.split("|")
            if len(parts) >= 3:
                std_name = parts[0].strip()
                display_name = parts[1].strip()
                group_name = parts[2].strip()
            elif len(parts) == 2:
                std_name = parts[0].strip()
                display_name = parts[1].strip()
                group_name = "默认分组"
            else:
                std_name = raw_line.strip()
                display_name = raw_line.strip()
                group_name = "默认分组"
            if std_name not in template_std_set:
                template_order_list.append(std_name)
                template_std_set.add(std_name)
                channel_info[std_name] = (display_name, group_name)
    print(f"【模板加载完成】模板有序频道总数：{len(template_order_list)}")
    return template_order_list, channel_info, template_std_set

# ====================== 生成待补充临时白名单 ======================
def build_low_channel_white(stock_set, template_set, stock_map):
    low_channel_set = set()
    # 1.模板存在但旧iptv完全缺失的频道
    missing_in_stock = template_set - stock_set
    for ch in missing_in_stock:
        low_channel_set.add(ch)
    # 2.旧iptv存在，但存量线路<MIN_STOCK_LINK
    for ch, link_list in stock_map.items():
        if ch in template_set and len(link_list) < MIN_STOCK_LINK:
            low_channel_set.add(ch)
    # 写入临时白名单文件
    with open(LOW_LINK_CHANNEL_FILE, "w", encoding="utf-8") as f:
        for ch in sorted(low_channel_set):
            f.write(ch + "\n")
    print(f"【临时白名单生成】待补充频道总数：{len(low_channel_set)}，路径：{LOW_LINK_CHANNEL_FILE}")
    return low_channel_set

# ====================== 黑名单工具 ======================
def load_blacklist():
    exact_black = set()
    fuzzy_keywords = []
    url_black = []
    if os.path.exists(BLACKLIST_FILE):
        with open(BLACKLIST_FILE, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if line.startswith("url*"):
                    word = line[4:].strip()
                    if word:
                        url_black.append(word)
                elif line.startswith("*"):
                    word = line[1:].strip()
                    if word:
                        fuzzy_keywords.append(word)
                else:
                    exact_black.add(line)
    print(f"【黑名单加载】精确频道:{len(exact_black)} 模糊关键词:{len(fuzzy_keywords)} URL广告:{len(url_black)}")
    return exact_black, fuzzy_keywords, url_black

def is_black_channel(channel_name, exact_black, fuzzy_keywords):
    if channel_name in exact_black:
        return True
    for kw in fuzzy_keywords:
        if kw in channel_name:
            return True
    return False

def is_black_url(url, url_black_list):
    for kw in url_black_list:
        if kw in url:
            return True
    return False

# ====================== 测速核心【优化：GET真实片段测速+重试机制】 ======================
def test_single_stream(url, url_black_list):
    if is_black_url(url, url_black_list):
        return (STREAM_REQ_TIMEOUT, url, False, True)
    latency = 999.9
    ok = False
    # 测速重试循环，成功直接跳出，不浪费请求
    for _ in range(STREAM_RETRY_TIMES + 1):
        start = time.perf_counter()
        try:
            resp = requests.get(
                url,
                timeout=STREAM_REQ_TIMEOUT,
                allow_redirects=True,
                verify=False,
                headers={"Range": "bytes=0-1023"}
            )
            if resp.status_code in (200, 206):
                latency = time.perf_counter() - start
                ok = True
                break  # 测速成功直接退出循环，无需多余重试
        except Exception:
            continue
    return (latency, url, ok, False)

# ====================== 配置加载工具 ======================
def load_alias():
    alias_map = {}
    if os.path.exists(ALIAS_FILE):
        with open(ALIAS_FILE, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                parts = [p.strip() for p in line.split(",")]
                std_name = parts[0]
                for raw_name in parts:
                    alias_map[raw_name] = std_name
    print(f"【别名表加载】映射总量 {len(alias_map)}")
    return alias_map

def load_allow_list(file_path):
    allow_set = set()
    if os.path.exists(file_path):
        with open(file_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#"):
                    allow_set.add(line)
    print(f"【临时白名单加载】允许补充频道 {len(allow_set)}")
    return allow_set

def fetch_source_urls():
    url_list = []
    if os.path.exists(SOURCES_FILE):
        with open(SOURCES_FILE, "r", encoding="utf-8") as f:
            for line in f:
                u = line.strip()
                if u and not u.startswith("#"):
                    url_list.append(u)
    print(f"【源列表读取】待抓取订阅总数 {len(url_list)}")
    return url_list

# ====================== 订阅文本解析 ======================
def download_text(url):
    try:
        resp = requests.get(url, timeout=8, verify=False)
        resp.encoding = resp.apparent_encoding
        return resp.text
    except Exception as e:
        print(f"【警告】源下载失败 {url}, 错误：{str(e)[:60]}")
        return ""

def parse_m3u(content):
    result = defaultdict(list)
    lines = content.splitlines()
    name = ""
    for line in lines:
        line = line.strip()
        if line.startswith("#EXTINF"):
            if "," in line:
                name = line.split(",")[-1].strip()
        elif line.startswith("http"):
            link = line.strip()
            if name:
                clean_link = clean_url_strip_suffix(link)
                result[name].append(clean_link)
    return result

def parse_txt(content):
    result = defaultdict(list)
    lines = content.splitlines()
    for line in lines:
        line = line.strip()
        if "#" in line:
            line = line.split("#")[0].strip()
        if "," in line:
            parts = line.split(",", 1)
            if len(parts) != 2:
                continue
            ch_name, url = parts
            ch_name = ch_name.strip()
            url = url.strip()
            if url.startswith("http"):
                clean_link = clean_url_strip_suffix(url)
                result[ch_name].append(clean_link)
    return result

# ====================== M3U转DIYP TXT ======================
def m3u_to_tvbox_txt(m3u_content):
    group_data = OrderedDict()
    curr_group = "默认分组"
    curr_ch = ""
    for raw_line in m3u_content.splitlines():
        l = raw_line.strip()
        if not l:
            continue
        if l.startswith("#EXTINF"):
            g_match = re.search(r'group-title="([^"]+)"', l)
            if g_match:
                curr_group = g_match.group(1)
            if "," in l:
                curr_ch = l.split(",")[-1].strip()
                if re.match(r"^CCTV-\d+", curr_ch):
                    curr_ch = curr_ch.replace("-", "－", 1)
        elif l.startswith("http"):
            clean_url = clean_url_strip_suffix(l).strip()
            if curr_group not in group_data:
                group_data[curr_group] = []
            group_data[curr_group].append(f"{curr_ch},{clean_url}")
    out_lines = []
    for gname, ch_lines in group_data.items():
        out_lines.append(f"{gname},#genre#")
        out_lines.extend(ch_lines)
    return "\n".join(out_lines)

# ====================== 主程序入口 ======================
def main():
    print("========== IPTV定时分拣程序启动 ==========")
    # 步骤0 优先加载黑名单（存量过滤需要）
    exact_black, fuzzy_keywords, url_black_list = load_blacklist()
    # 步骤1 读取历史iptv存量，同步过滤存量广告URL
    stock_channel_map, full_stock_channel_set, stock_filter_black = parse_old_iptv_stock(url_black_list)
    # 步骤2 读取模板有序列表、频道名称分组信息
    template_order_list, channel_info, template_std_set = load_template_info()
    # 步骤3 生成两类待补充频道临时白名单
    low_channel_set = build_low_channel_white(full_stock_channel_set, template_std_set, stock_channel_map)

    alias_map = load_alias()
    source_urls = fetch_source_urls()

    # 加载临时白名单，本轮仅抓取该名单内频道
    allow_set = load_allow_list(LOW_LINK_CHANNEL_FILE)

    source_statistics = defaultdict(lambda: {"total": 0, "good": 0, "fallback": 0, "black": 0})
    url_belong_source = defaultdict(list)
    raw_all_channels = defaultdict(list)

    # 拉取所有外部订阅源
    for idx, src in enumerate(source_urls, start=1):
        print(f"\n【{idx}/{len(source_urls)}】抓取订阅源：{src}")
        txt = download_text(src)
        if not txt:
            print(f"【跳过】源无返回内容")
            source_statistics[src]["total"] = 0
            continue
        if "#EXTM3U" in txt:
            data = parse_m3u(txt)
        else:
            data = parse_txt(txt)
        src_total = sum(len(v) for v in data.values())
        source_statistics[src]["total"] = src_total
        print(f"【源解析完成】频道数：{len(data)} 原始线路：{src_total}")
        for ch, urls in data.items():
            for u in urls:
                url_belong_source[u].append(src)
            raw_all_channels[ch].extend(urls)
    print(f"\n【全部源抓取完毕】原始解析频道总数：{len(raw_all_channels)}")

    # 一级去重
    dedup_raw_channels = defaultdict(list)
    for ch_name, url_list in raw_all_channels.items():
        unique_urls = list(dict.fromkeys(url_list))
        dedup_raw_channels[ch_name] = unique_urls
    print(f"【一级去重完成】待预处理频道：{len(dedup_raw_channels)}")

    # 别名转换 + 黑白名单过滤 + 仅保留临时白名单频道
    std_channels = defaultdict(list)
    drop_black_channel_count = 0
    for raw_name, urllist in dedup_raw_channels.items():
        std_name = alias_map.get(raw_name, raw_name)
        if is_black_channel(std_name, exact_black, fuzzy_keywords):
            print(f"【频道黑名单剔除】{std_name}")
            drop_black_channel_count += 1
            continue
        if allow_set and std_name not in allow_set:
            continue
        std_channels[std_name].extend(urllist)
        all_urls = std_channels[std_name]
        final_unique = list(dict.fromkeys(all_urls))
        before = len(all_urls)
        after = len(final_unique)
        std_channels[std_name] = final_unique
        if before > after:
            print(f"【频道二次去重】{std_name} 清理重复 {before - after} 条")
    print(f"【频道预处理结束】黑名单丢弃{drop_black_channel_count}个，进入测速频道：{len(std_channels)}")

    # 批量测速（使用优化后的低并发线程数）
    channel_all_test_result = defaultdict(list)
    all_test_tasks = []
    for ch_name, urllist in std_channels.items():
        for link in urllist:
            all_test_tasks.append((ch_name, link))

    total_task_num = len(all_test_tasks)
    print(f"\n【启动批量测速】待检测链接总数：{total_task_num}")
    finished = 0
    with ThreadPoolExecutor(max_workers=STREAM_REQ_WORKERS) as pool:
        futures_map = {}
        for ch, link in all_test_tasks:
            f = pool.submit(test_single_stream, link, url_black_list)
            futures_map[f] = (ch, link)
        for future in as_completed(futures_map):
            ch_name, url = futures_map[future]
            latency, url, ok, hit_url_black = future.result()
            finished += 1
            if hit_url_black or finished % 20 == 0:
                print(f"【测速进度 {finished}/{total_task_num}】")
            if hit_url_black:
                print(f"  >> 【广告URL过滤】{url}")
                belong_src_list = url_belong_source.get(url, [])
                for s in belong_src_list:
                    source_statistics[s]["black"] += 1
                continue
            channel_all_test_result[ch_name].append((latency, url, ok))
            belong_src_list = url_belong_source.get(url, [])
            for belong_src in belong_src_list:
                if ok:
                    if latency <= LATENCY_THRESHOLD:
                        source_statistics[belong_src]["good"] += 1
                    else:
                        source_statistics[belong_src]["fallback"] += 1
    print("【全部测速任务执行完成】\n")

    # 按模板顺序合并线路（严格遵循模板顺序输出，复用前置加载模板缓存，消除重复IO）
    final_channel_data = OrderedDict()
    matched_count = 0
    template_queue = template_order_list

    for std_name in template_queue:
        # 黑名单频道直接跳过
        if is_black_channel(std_name, exact_black, fuzzy_keywords):
            continue
        show_name, group = channel_info[std_name]
        stock_info_list = stock_channel_map.get(std_name, [])
        stock_url_set = set(u for _, u, _ in stock_info_list)

        # 判断是否需要补充新线路
        if std_name in low_channel_set:
            new_test_items = channel_all_test_result.get(std_name, [])
            new_good = []
            new_fallback = []
            for lat, u, ok in new_test_items:
                if not ok:
                    continue
                if u in stock_url_set:
                    continue
                if lat <= LATENCY_THRESHOLD:
                    new_good.append((lat, u, True))
                else:
                    new_fallback.append((lat, u, False))
            new_good.sort(key=lambda x: x[0])
            new_fallback.sort(key=lambda x: x[0])
            merge_all = new_good + new_fallback + stock_info_list
        else:
            # 存量充足频道，仅保留原有线路，不补充新链接
            merge_all = stock_info_list

        # 全局URL去重
        unique_check = []
        exist_url = set()
        for item in merge_all:
            _, url, _ = item
            if url not in exist_url:
                exist_url.add(url)
                unique_check.append(item)
        merge_all = unique_check
        # 单频道最多保留8条
        final_items = merge_all[:MAX_LINK_PER_CHANNEL]
        if len(final_items) == 0:
            continue
        matched_count += 1
        final_channel_data[std_name] = (show_name, group, final_items)
        print(f"【频道合并完成】[{show_name}] 最终留存线路 {len(final_items)} 条")

    # 组装标准M3U
    lines = ["#EXTM3U x-tvg-url=\"epg.xml.gz\""]
    for ch_std, (show_name, group, link_items) in final_channel_data.items():
        for lat, url, _ in link_items:
            lines.append(f'#EXTINF:-1 group-title="{group}",{show_name}')
            lines.append(url)

    # 输出iptv.txt
    with open(OUTPUT_TXT, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    # 输出DIYP tv.txt
    with open(OUTPUT_TXT, "r", encoding="utf-8") as rf:
        m3u_full = rf.read()
    tvbox_text = m3u_to_tvbox_txt(m3u_full)
    with open(TV_BOX_OUTPUT, "w", encoding="utf-8") as wf:
        wf.write(tvbox_text)
    print(f"\n【DIYP输出完成】文件：{TV_BOX_OUTPUT}")

    valid_channel_cnt = sum(1 for _, items in channel_all_test_result.items() if any(ok for _, _, ok in items))

    # 打印源统计报表
    print("\n===================== 订阅源链路统计总表 =====================")
    for source_url, stat in source_statistics.items():
        print(f"\n订阅源地址：{source_url}")
        print(f"  ① 原始抓取总线路：{stat['total']} 条")
        print(f"  ② 广告URL过滤线路：{stat['black']} 条")
        print(f"  ③ 快速优质链路(≤0.8s)：{stat['good']} 条")
        print(f"  ④ 慢速兜底可用链路：{stat['fallback']} 条")
        print(f"  ✅ 该源总有效可用线路：{stat['good'] + stat['fallback']} 条")
    print("==============================================================\n")

    # 全局汇总
    print(f"\n========== 全局执行汇总 ==========")
    print(f"测速存在连通链路的频道总数: {valid_channel_cnt}")
    print(f"模板内成功输出频道总数: {matched_count}")
    print(f"标准M3U输出文件：{OUTPUT_TXT}")
    print(f"DIYP分组文件：{TV_BOX_OUTPUT}")
    print(f"缺量/缺失频道临时白名单：{LOW_LINK_CHANNEL_FILE}")
    print(f"历史存量过滤广告链接总数：{stock_filter_black} 条")
    print("==== IPTV分拣程序全部执行完毕 ====")

if __name__ == "__main__":
    main()
