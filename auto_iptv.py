import os
import time
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
STREAM_REQ_TIMEOUT = 3.5    # 延长超时，过滤不稳定链接
STREAM_EVAL_WORKERS = 6
MAX_LINK_PER_CHANNEL = 10     # 减少单频道有效链接数量，只保留最优节点
FALLBACK_MAX_LINK = 3        # 无有效链接时兜底最多保留3条
LATENCY_THRESHOLD = 1.2      # 新增：延迟高于1.2s即使连通也降级为备选
# ======================================================

# 屏蔽不必要警告
import warnings
warnings.filterwarnings("ignore")
import requests
requests.packages.urllib3.disable_warnings()

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

def load_blacklist():
    black_set = set()
    if os.path.exists(BLACKLIST_FILE):
        with open(BLACKLIST_FILE, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#"):
                    black_set.add(line)
    return black_set

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

def test_single_stream(url):
    """一次性测速：返回 (延迟, url, 是否连通)，全局仅调用一次，消除重复请求"""
    start = time.perf_counter()
    ok = False
    try:
        resp = requests.head(url, timeout=STREAM_REQ_TIMEOUT, verify=False, allow_redirects=True)
        if 200 <= resp.status_code < 400:
            ok = True
    except Exception:
        pass
    latency = time.perf_counter() - start
    return (latency, url, ok)

def main():
    alias_map = load_alias()
    allow_set = load_allow_list()
    black_set = load_blacklist()
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

    # 别名标准化 + 链接全局去重
    std_channels = defaultdict(list)
    for raw_name, urllist in raw_all_channels.items():
        std_name = alias_map.get(raw_name, raw_name)
        if allow_set and std_name not in allow_set:
            continue
        if std_name in black_set:
            continue
        # 链接去重
        unique_links = list(dict.fromkeys(urllist))
        std_channels[std_name].extend(unique_links)

    # ============ 全局一次性测速（只请求一次链接，架构核心优化） ============
    channel_all_test_result = defaultdict(list)
    all_test_tasks = []
    for ch_name, urllist in std_channels.items():
        for link in urllist:
            all_test_tasks.append((ch_name, link))

    print(f"开始批量测速，总待检测链接数量：{len(all_test_tasks)}")
    with ThreadPoolExecutor(max_workers=STREAM_EVAL_WORKERS) as pool:
        futures_map = {}
        for ch, link in all_test_tasks:
            f = pool.submit(test_single_stream, link)
            futures_map[f] = (ch, link)
        for future in as_completed(futures_map):
            ch_name, _ = futures_map[future]
            latency, url, ok = future.result()
            channel_all_test_result[ch_name].append((latency, url, ok))

    # ============ 读取template配置 ============
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
    # 严格按照template顺序输出频道
    for std_name in template_queue:
        test_results = channel_all_test_result.get(std_name, [])
        if not test_results:
            continue

        valid_items = []
        invalid_items = []
        for latency, url, ok in test_results:
            if ok:
                # 关键优化：连通但是延迟过高 → 划入备选池，不作为优先链接
                if latency <= LATENCY_THRESHOLD:
                    valid_items.append((latency, url))
                else:
                    invalid_items.append((latency, url))
            else:
                invalid_items.append((latency, url))

        show_name, group = channel_info[std_name]
        if valid_items:
            # 场景1：存在低延迟优质可用链接 → 按延迟升序择优
            valid_items.sort(key=lambda x: x[0])
            output_links = [item[1] for item in valid_items[:MAX_LINK_PER_CHANNEL]]
            print(f"【正常输出】频道[{show_name}] 使用优质有效链接，数量:{len(output_links)}")
            for link in output_links:
                lines.append(f'#EXTINF:-1 group-title="{group}",{show_name}')
                lines.append(link)
            matched_count += 1
        else:
            # 场景2：无低延迟优质链接，触发兜底降级
            if not invalid_items:
                continue
            invalid_items.sort(key=lambda x: x[0])
            unique_fallback = list(dict.fromkeys([u for _, u in invalid_items]))
            output_links = unique_fallback[:FALLBACK_MAX_LINK]
            print(f"【降级兜底】频道[{show_name}] 无优质链接，保留{len(output_links)}条备选")
            for link in output_links:
                lines.append(f'#EXTINF:-1 group-title="{group}",{show_name}')
                lines.append(link)
            matched_count += 1

    # 写入输出文件
    with open(OUTPUT_TXT, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    valid_channel_cnt = sum(1 for _, items in channel_all_test_result.items() if any(ok for _, _, ok in items))
    print(f"\n【执行汇总】")
    print(f"测速筛选得到存在连通链接的频道总数: {valid_channel_cnt}")
    print(f"template列表内成功输出的频道总数: {matched_count}")
    print("==== IPTV分拣流程全部结束 ====")

if __name__ == "__main__":
    main()
