import os
import time
import re
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

# ====================== 全局配置区 ======================
BASE_PATH = "config/"
SOURCES_FILE = os.path.join(BASE_PATH, "sources.txt")
ALIAS_FILE = os.path.join(BASE_PATH, "alias.txt")
ALLOW_LIST_FILE = os.path.join(BASE_PATH, "allow_list.txt")
TEMPLATE_OUTPUT_FILE = os.path.join(BASE_PATH, "template_output.txt")
BLACKLIST_FILE = os.path.join(BASE_PATH, "blacklist.txt")
OUTPUT_TXT = "iptv.txt"

# 测速参数【统一常量】
STREAM_REQ_TIMEOUT = 3.5
STREAM_EVAL_WORKERS = 6
MAX_LINK_PER_CHANNEL = 3
FALLBACK_MAX_LINK = 3
LATENCY_THRESHOLD = 1.2
MIN_VERTICAL_RES = 720
# ======================================================

import warnings
warnings.filterwarnings("ignore")
import requests
requests.packages.urllib3.disable_warnings()

RESOLUTION_REG = re.compile(r'RESOLUTION=(\d+)x(\d+)', re.IGNORECASE)
DISCONTINUITY_REG = re.compile(r'#EXT-X-DISCONTINUITY', re.IGNORECASE)

def load_blacklist():
    exact_black = set()
    fuzzy_keywords = []
    url_black = [] # URL链接黑名单关键词
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
    return exact_black, fuzzy_keywords, url_black

def is_black_channel(channel_name, exact_black, fuzzy_keywords):
    if channel_name in exact_black:
        return True
    for kw in fuzzy_keywords:
        if kw in channel_name:
            return True
    return False

def is_black_url(url, url_black_list):
    """判断播放链接是否命中URL黑名单"""
    for kw in url_black_list:
        if kw in url:
            return True
    return False

def has_ad_discontinuity(url):
    """检测m3u8是否存在广告切换标签（插播广告）"""
    try:
        resp = requests.get(url, timeout=2, verify=False)
        text = resp.text
        if DISCONTINUITY_REG.search(text):
            return True
    except Exception:
        pass
    return False

def get_stream_resolution(url):
    try:
        resp = requests.get(url, timeout=2.5, verify=False)
        text = resp.text
        matches = RESOLUTION_REG.findall(text)
        if matches:
            w, h = matches[0]
            return int(h)
    except Exception:
        pass
    return None

def test_single_stream(url, url_black_list):
    start = time.perf_counter()
    # 优先判断URL黑名单，直接拦截广告链接
    if is_black_url(url, url_black_list):
        return (time.perf_counter() - start, url, False, None, True)
    ok = False
    try:
        resp = requests.head(url, timeout=STREAM_REQ_TIMEOUT, allow_redirects=True, verify=False)
        if 200 <= resp.status_code < 400:
            ok = True
    except Exception:
        pass
    latency = time.perf_counter() - start
    height = None
    if ok:
        height = get_stream_resolution(url)
    return (latency, url, ok, height, False)

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
    return alias_map

def load_allow_list():
    allow_set = set()
    if os.path.exists(ALLOW_LIST_FILE):
        with open(ALLOW_LIST_FILE, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#"):
                    allow_set.add(line)
    return allow_set

def fetch_source_urls():
    url_list = []
    if os.path.exists(SOURCES_FILE):
        with open(SOURCES_FILE, "r", encoding="utf-8") as f:
            for line in f:
                u = line.strip()
                if u and not u.startswith("#"):
                    url_list.append(u)
    return url_list

def download_text(url):
    try:
        resp = requests.get(url, timeout=8, verify=False)
        resp.encoding = resp.apparent_encoding
        return resp.text
    except Exception as e:
        print(f"【警告】源地址下载失败 {url}, err: {str(e)[:60]}")
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
                result[name].append(link)
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
                result[ch_name].append(url)
    return result

def main():
    alias_map = load_alias()
    allow_set = load_allow_list()
    exact_black, fuzzy_keywords, url_black_list = load_blacklist()
    source_urls = fetch_source_urls()

    raw_all_channels = defaultdict(list)
    for src in source_urls:
        print(f"正在拉取源: {src}")
        txt = download_text(src)
        if "#EXTM3U" in txt:
            data = parse_m3u(txt)
        else:
            data = parse_txt(txt)
        for ch, urls in data.items():
            raw_all_channels[ch].extend(urls)

    std_channels = defaultdict(list)
    for raw_name, urllist in raw_all_channels.items():
        std_name = alias_map.get(raw_name, raw_name)
        # 拦截黑名单频道
        if is_black_channel(std_name, exact_black, fuzzy_keywords):
            print(f"【频道黑名单剔除】{std_name}")
            continue
        if allow_set and std_name not in allow_set:
            continue
        unique_links = list(dict.fromkeys(urllist))
        std_channels[std_name].extend(unique_links)

    channel_all_test_result = defaultdict(list)
    all_test_tasks = []
    for ch_name, urllist in std_channels.items():
        for link in urllist:
            all_test_tasks.append((ch_name, link))

    print(f"开始批量测速，总待检测链接数量：{len(all_test_tasks)}")
    with ThreadPoolExecutor(max_workers=STREAM_EVAL_WORKERS) as pool:
        futures_map = {}
        for ch, link in all_test_tasks:
            f = pool.submit(test_single_stream, link, url_black_list)
            futures_map[f] = (ch, link)
        for future in as_completed(futures_map):
            ch_name, url = futures_map[future]
            latency, url, ok, height, hit_url_black = future.result()
            if hit_url_black:
                print(f"【URL黑名单剔除广告链接】{url}")
                continue
            channel_all_test_result[ch_name].append((latency, url, ok, height))

    lines = ["#EXTM3U x-tvg-url=\"epg.xml.gz\""]
    template_queue = []
    channel_info = {}
    if os.path.exists(TEMPLATE_OUTPUT_FILE):
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
                template_queue.append(std_name)
                channel_info[std_name] = (display_name, group_name)

    matched_count = 0
    for std_name in template_queue:
        test_results = channel_all_test_result.get(std_name, [])
        if not test_results:
            continue
        valid_items = []
        invalid_items = []
        for latency, url, ok, height in test_results:
            if ok:
                if height is not None and height < MIN_VERTICAL_RES:
                    invalid_items.append((latency, url))
                else:
                    if latency <= LATENCY_THRESHOLD:
                        valid_items.append((latency, url))
                    else:
                        invalid_items.append((latency, url))
            else:
                invalid_items.append((latency, url))

        show_name, group = channel_info[std_name]
        if valid_items:
            valid_items.sort(key=lambda x: x[0])
            output_links = [item[1] for item in valid_items[:MAX_LINK_PER_CHANNEL]]
            print(f"【正常输出】频道[{show_name}] 优质链接{len(output_links)}条")
            for link in output_links:
                lines.append(f'#EXTINF:-1 group-title="{group}",{show_name}')
                lines.append(link)
            matched_count += 1
        else:
            if not invalid_items:
                continue
            invalid_items.sort(key=lambda x: x[0])
            unique_fallback = list(dict.fromkeys([u for _, u in invalid_items]))
            output_links = unique_fallback[:FALLBACK_MAX_LINK]
            print(f"【降级兜底】频道[{show_name}] 仅保留备选链接")
            for link in output_links:
                lines.append(f'#EXTINF:-1 group-title="{group}",{show_name}')
                lines.append(link)

    with open(OUTPUT_TXT, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    valid_channel_cnt = sum(1 for _, items in channel_all_test_result.items() if any(ok for _, _, ok, _ in items))
    print(f"\n【执行汇总】")
    print(f"测速有效频道: {valid_channel_cnt}")
    print(f"模板匹配输出频道: {matched_count}")
    print("==== IPTV分拣完成 ====")

if __name__ == "__main__":
    main()
