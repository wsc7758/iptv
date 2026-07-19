import os
import re
import time
from collections import defaultdict, OrderedDict
from concurrent.futures import ThreadPoolExecutor, as_completed

# ====================== 全局配置区 ======================
BASE_PATH = "config/"
SOURCES_FILE = os.path.join(BASE_PATH, "sources.txt")
ALIAS_FILE = os.path.join(BASE_PATH, "alias.txt")
ALLOW_LIST_FILE = os.path.join(BASE_PATH, "allow_list.txt")
TEMPLATE_OUTPUT_FILE = os.path.join(BASE_PATH, "template_output.txt")
BLACKLIST_FILE = os.path.join(BASE_PATH, "blacklist.txt")
OUTPUT_TXT = "iptv.txt"
TV_BOX_OUTPUT = "tv.txt"

# 测速参数【统一常量】
STREAM_REQ_TIMEOUT = 3.5
STREAM_EVAL_WORKERS = 6   # 降低并发，防止容器网络拥堵
MAX_LINK_PER_CHANNEL = 10
FALLBACK_MAX_LINK = 5
LATENCY_THRESHOLD = 1.2
# ======================================================

import warnings
warnings.filterwarnings("ignore")
import requests
requests.packages.urllib3.disable_warnings

# ====================== 新增URL标准化清洗函数 ======================
def get_standard_url(raw_url: str) -> str:
    """
    标准化URL，去除线路标记后缀，用于查重对比
    清除 $LR•xxxx『线路xx』 这类不影响播放的自定义备注
    """
    # 匹配 $ 开头到行尾的所有自定义线路标记
    std_url = re.sub(r"\$.*$", "", raw_url.strip())
    return std_url
# ==================================================================

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
    print(f"【黑名单加载完成】精确频道黑名单:{len(exact_black)}条，模糊关键词:{len(fuzzy_keywords)}条，URL广告黑名单:{len(url_black)}条")
    return exact_black, fuzzy_keywords, url_black_list

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

def test_single_stream(url, url_black_list):
    start = time.perf_counter()
    if is_black_url(url, url_black_list):
        return (time.perf_counter() - start, url, False, True)
    ok = False
    try:
        resp = requests.head(url, timeout=STREAM_REQ_TIMEOUT, allow_redirects=True, verify=False)
        if 200 <= resp.status_code < 400:
            ok = True
    except Exception:
        pass
    latency = time.perf_counter() - start
    return (latency, url, ok, False)

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
    print(f"【别名表加载完成】共载入 {len(alias_map)} 条别名映射")
    return alias_map

def load_allow_list():
    allow_set = set()
    if os.path.exists(ALLOW_LIST_FILE):
        with open(ALLOW_LIST_FILE, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#"):
                    allow_set.add(line)
    print(f"【白名单加载完成】允许频道总数: {len(allow_set)}")
    return allow_set

def fetch_source_urls():
    url_list = []
    if os.path.exists(SOURCES_FILE):
        with open(SOURCES_FILE, "r", encoding="utf-8") as f:
            for line in f:
                u = line.strip()
                if u and not u.startswith("#"):
                    url_list.append(u)
    print(f"【源列表读取完成】待拉取源总数：{len(url_list)}")
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

# M3U转DIYP标准txt格式工具函数
def m3u_to_tvbox_txt(m3u_content):
    group_data = OrderedDict()
    curr_group = "默认分组"
    curr_ch = ""
    for raw_line in m3u_content.splitlines():
        l = raw_line.strip()
        if not l:
            continue
        if l.startswith("#EXTINF"):
            # 提取分组名称
            g_match = re.search(r'group-title="([^"]+)"', l)
            if g_match:
                curr_group = g_match.group(1)
            # 提取频道名称
            if "," in l:
                curr_ch = l.split(",")[-1].strip()
                # 匹配所有CCTV-数字开头频道，半角-替换成全角－，不用改模板/白名单
                if re.match(r"^CCTV-\d+", curr_ch):
                    curr_ch = curr_ch.replace("-", "－", 1)
        elif l.startswith("http"):
            # 清除链接尾部$线路备注垃圾字符
            clean_url = re.sub(r"\$.*", "", l).strip()
            if curr_group not in group_data:
                group_data[curr_group] = []
            group_data[curr_group].append(f"{curr_ch},{clean_url}")
    # 组装输出文本
    out_lines = []
    for gname, ch_lines in group_data.items():
        out_lines.append(f"{gname},#genre#")
        out_lines.extend(ch_lines)
    return "\n".join(out_lines)

def main():
    print("========== IPTV分拣程序开始运行 ==========")
    alias_map = load_alias()
    allow_set = load_allow_list()
    exact_black, fuzzy_keywords, url_black_list = load_blacklist()
    source_urls = fetch_source_urls()

    raw_all_channels = defaultdict(list)
    for idx, src in enumerate(source_urls, start=1):
        print(f"\n【{idx}/{len(source_urls)}】正在拉取源: {src}")
        txt = download_text(src)
        if not txt:
            print(f"【跳过】该源无返回内容")
            continue
        if "#EXTM3U" in txt:
            data = parse_m3u(txt)
        else:
            data = parse_txt(txt)
        print(f"【源解析完成】该源解析出频道数量：{len(data)}")
        for ch, urls in data.items():
            raw_all_channels[ch].extend(urls)
    print(f"\n【全部源拉取完毕】原始未清洗频道总数：{len(raw_all_channels)}")

    # ====================== 核心优化：源头标准化去重 ======================
    # 用标准化URL查重，提前剔除带$线路标记的重复链接，大幅减少后续测速任务
    dedup_raw_channels = defaultdict(list)
    for ch_name, url_list in raw_all_channels.items():
        seen_std_url = set()
        unique_urls = []
        for raw_url in url_list:
            std_u = get_standard_url(raw_url)
            if std_u not in seen_std_url:
                seen_std_url.add(std_u)
                unique_urls.append(raw_url)
        dedup_raw_channels[ch_name] = unique_urls
    print(f"【源头URL标准化清洗完成】清洗后待处理频道总数：{len(dedup_raw_channels)}")
    # ====================================================================

    std_channels = defaultdict(list)
    drop_black_channel_count = 0
    for raw_name, urllist in dedup_raw_channels.items():  # 替换为清洗后的频道字典
        std_name = alias_map.get(raw_name, raw_name)
        if is_black_channel(std_name, exact_black, fuzzy_keywords):
            print(f"【频道黑名单剔除】{std_name}")
            drop_black_channel_count += 1
            continue
        if allow_set and std_name not in allow_set:
            continue
        # 合并链接并二次兜底去重（防止多源同名频道残留重复）
        std_channels[std_name].extend(urllist)
        all_urls = std_channels[std_name]
        # 复用标准化函数二次查重
        final_unique = []
        seen = set()
        for u in all_urls:
            su = get_standard_url(u)
            if su not in seen:
                seen.add(su)
                final_unique.append(u)
        before = len(all_urls)
        after = len(final_unique)
        std_channels[std_name] = final_unique
        if before > after:
            print(f"【频道二次去重】{std_name} 清除重复线路 {before - after} 条")
    print(f"【频道预处理结束】黑名单丢弃频道总数:{drop_black_channel_count}，进入测速频道总数:{len(std_channels)}")

    channel_all_test_result = defaultdict(list)
    all_test_tasks = []
    for ch_name, urllist in std_channels.items():
        for link in urllist:
            all_test_tasks.append((ch_name, link))

    total_task_num = len(all_test_tasks)
    print(f"\n【开始批量测速】总待检测链接数量：{total_task_num}")
    finished = 0
    with ThreadPoolExecutor(max_workers=STREAM_EVAL_WORKERS) as pool:
        futures_map = {}
        for ch, link in all_test_tasks:
            f = pool.submit(test_single_stream, link, url_black_list)
            futures_map[f] = (ch, link)
        for future in as_completed(futures_map):
            ch_name, url = futures_map[future]
            latency, url, ok, hit_url_black = future.result()
            finished += 1
            if hit_url_black or finished % 20 == 0:
                print(f"【测速进度 {finished}/{total_task_num}】已完成链接测速")
            if hit_url_black:
                print(f"  >> 【URL黑名单剔除广告链接】{url}")
                continue
            channel_all_test_result[ch_name].append((latency, url, ok))
    print("【全部测速任务执行完毕】\n")

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
    print(f"【模板读取完成】模板内待输出频道总数：{len(template_queue)}")

    matched_count = 0
    for std_name in template_queue:
        test_results = channel_all_test_result.get(std_name, [])
        if not test_results:
            continue
        valid_items = []
        invalid_items = []
        for latency, url, ok in test_results:
            if ok:
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
            # 兜底线路同样标准化去重
            seen_std = set()
            unique_fallback = []
            for _, u in invalid_items:
                su = get_standard_url(u)
                if su not in seen_std:
                    seen_std.add(su)
                    unique_fallback.append(u)
            output_links = unique_fallback[:FALLBACK_MAX_LINK]
            print(f"【降级兜底】频道[{show_name}] 仅保留备选链接{len(output_links)}条")
            for link in output_links:
                lines.append(f'#EXTINF:-1 group-title="{group}",{show_name}')
                lines.append(link)

    # 写入M3U格式 iptv.txt
    with open(OUTPUT_TXT, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    # 转换生成DIYP专用 tv.txt
    with open(OUTPUT_TXT, "r", encoding="utf-8") as rf:
        m3u_full = rf.read()
    tvbox_text = m3u_to_tvbox_txt(m3u_full)
    with open(TV_BOX_OUTPUT, "w", encoding="utf-8") as wf:
        wf.write(tvbox_text)
    print(f"\n【额外生成完成】DIYP专用订阅文件：{TV_BOX_OUTPUT}")

    valid_channel_cnt = sum(1 for _, items in channel_all_test_result.items() if any(ok for _, _, ok in items))
    print(f"\n========== 执行汇总 ==========")
    print(f"测速存在连通链接的频道总数: {valid_channel_cnt}")
    print(f"template模板内成功输出频道总数: {matched_count}")
    print(f"M3U通用文件：{OUTPUT_TXT}")
    print(f"DIYP专用分组文件：{TV_BOX_OUTPUT}")
    print("==== IPTV分拣程序全部结束 ====")

if __name__ == "__main__":
    main()
