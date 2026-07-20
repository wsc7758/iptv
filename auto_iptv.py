import os
import re
import time
import gc
import sys
import traceback
from collections import defaultdict, OrderedDict
from concurrent.futures import ThreadPoolExecutor, as_completed

# ====================== 全局配置区【常量统一集中】 ======================
BASE_PATH = "config/"
SOURCES_FILE = os.path.join(BASE_PATH, "sources.txt")
ALIAS_FILE = os.path.join(BASE_PATH, "alias.txt")
ALLOW_LIST_FILE = os.path.join(BASE_PATH, "allow_list.txt")
TEMPLATE_OUTPUT_FILE = os.path.join(BASE_PATH, "template_output.txt")
BLACKLIST_FILE = os.path.join(BASE_PATH, "blacklist.txt")
OUTPUT_TXT = "iptv.txt"
TV_BOX_OUTPUT = "tv.txt"

# 测速参数【适配GitHub Actions海外弱网】
STREAM_REQ_TIMEOUT = 2.0
STREAM_EVAL_WORKERS = 2
MAX_LINK_PER_CHANNEL = 8
FALLBACK_MAX_LINK = 5
LATENCY_THRESHOLD = 2.5
HTTP_RANGE_HEADERS = {"Range": "bytes=0-1023"}

# 正则预编译
REGEX_CCTV_PREFIX = re.compile(r"^CCTV-\d+")
REGEX_URL_SUFFIX_CLEAN = re.compile(r"\$.*$")
REGEX_GROUP_TITLE = re.compile(r'group-title="([^"]+)"')
# ======================================================================

import warnings
warnings.filterwarnings("ignore")
import requests
requests.packages.urllib3.disable_warnings()

# ====================== 通用工具函数模块 ======================
def clean_url_strip_suffix(raw_url: str) -> str:
    clean = raw_url.strip()
    return REGEX_URL_SUFFIX_CLEAN.sub("", clean)

def safe_read_file(file_path: str, encoding: str = "utf-8") -> str:
    if not os.path.exists(file_path):
        return ""
    try:
        with open(file_path, "r", encoding=encoding) as f:
            return f.read()
    except Exception as e:
        print(f"【文件读取异常】{file_path}, err: {str(e)[:60]}")
        return ""

def unique_list(raw_list: list) -> list:
    return list(dict.fromkeys(raw_list))

def append_bad_url_to_blacklist(bad_url_set: set):
    if not os.path.exists(BASE_PATH):
        os.makedirs(BASE_PATH)
    if not bad_url_set:
        print("【无新增失效链接，无需更新黑名单】")
        return
    old_content = safe_read_file(BLACKLIST_FILE)
    exist_urls = set()
    for line in old_content.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("url*"):
            exist_urls.add(line[4:].strip())
    new_bad_urls = [u for u in bad_url_set if u not in exist_urls]
    if not new_bad_urls:
        print("【所有失效链接已存在于黑名单，无需追加】")
        return
    write_lines = ["# 自动测速检测失效链接 url|auto_bad"]
    write_lines.extend([f"url*{u}" for u in new_bad_urls])
    try:
        with open(BLACKLIST_FILE, "a", encoding="utf-8") as f:
            f.write("\n" + "\n".join(write_lines) + "\n")
        print(f"【自动黑名单写入完成】新增{len(new_bad_urls)}条完全失效播放链接")
    except Exception as e:
        # 关键修复：写入失败仅警告，不中断整个分拣流程
        print(f"【警告：黑名单写入失败，本次不自动拉黑失效链接】err: {str(e)[:80]}")
# ==================================================================

def load_blacklist():
    exact_black = set()
    fuzzy_keywords = []
    url_black = []
    file_content = safe_read_file(BLACKLIST_FILE)
    for line in file_content.splitlines():
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
    return exact_black, fuzzy_keywords, url_black

def is_black_channel(channel_name, exact_black, fuzzy_keywords):
    if channel_name in exact_black:
        return True
    for kw in fuzzy_keywords:
        if kw in channel_name:
            return True
    return False

def is_black_url(url, url_black_list):
    return url in url_black_list

def test_single_stream(url, url_black_list):
    start = time.perf_counter()
    if is_black_url(url, url_black_list):
        return (time.perf_counter() - start, url, False, True)
    ok = False
    try:
        resp = requests.get(
            url,
            timeout=STREAM_REQ_TIMEOUT,
            allow_redirects=True,
            verify=False,
            headers=HTTP_RANGE_HEADERS
        )
        if 200 <= resp.status_code < 400:
            ok = True
    except Exception:
        pass
    latency = time.perf_counter() - start
    return (latency, url, ok, False)

def load_alias():
    alias_map = {}
    file_content = safe_read_file(ALIAS_FILE)
    for line in file_content.splitlines():
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
    file_content = safe_read_file(ALLOW_LIST_FILE)
    for line in file_content.splitlines():
        line = line.strip()
        if line and not line.startswith("#"):
            allow_set.add(line)
    print(f"【白名单加载完成】允许频道总数: {len(allow_set)}")
    return allow_set

def fetch_source_urls():
    url_list = []
    file_content = safe_read_file(SOURCES_FILE)
    for line in file_content.splitlines():
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

def m3u_to_tvbox_txt(m3u_content):
    group_data = OrderedDict()
    curr_group = "默认分组"
    curr_ch = ""
    for raw_line in m3u_content.splitlines():
        l = raw_line.strip()
        if not l:
            continue
        if l.startswith("#EXTINF"):
            g_match = REGEX_GROUP_TITLE.search(l)
            if g_match:
                curr_group = g_match.group(1)
            if "," in l:
                curr_ch = l.split(",")[-1].strip()
                if REGEX_CCTV_PREFIX.match(curr_ch):
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

def main():
    print("========== IPTV分拣程序开始运行 ==========")
    if not os.path.exists(BASE_PATH):
        os.makedirs(BASE_PATH)
        print(f"【自动创建配置目录】{BASE_PATH}")

    alias_map = load_alias()
    allow_set = load_allow_list()
    exact_black, fuzzy_keywords, url_black_list = load_blacklist()
    source_urls = fetch_source_urls()

    source_statistics = defaultdict(lambda: {"total": 0, "good": 0, "fallback": 0, "black": 0})
    url_belong_source = defaultdict(list)
    raw_all_channels = defaultdict(list)
    auto_bad_url_set = set()

    for idx, src in enumerate(source_urls, start=1):
        print(f"\n【{idx}/{len(source_urls)}】正在拉取源: {src}")
        txt = download_text(src)
        if not txt:
            print(f"【跳过】该源无返回内容")
            source_statistics[src]["total"] = 0
            continue
        data = parse_m3u(txt) if "#EXTM3U" in txt else parse_txt(txt)
        src_total = sum(len(v) for v in data.values())
        source_statistics[src]["total"] = src_total
        print(f"【源解析完成】该源解析出频道数量：{len(data)}，原始线路总数：{src_total}")
        for ch, urls in data.items():
            for u in urls:
                url_belong_source[u].append(src)
            raw_all_channels[ch].extend(urls)
    print(f"\n【全部源拉取完毕】清洗后原始频道总数：{len(raw_all_channels)}")

    dedup_raw_channels = defaultdict(list)
    for ch_name, url_list in raw_all_channels.items():
        dedup_raw_channels[ch_name] = unique_list(url_list)
    print(f"【一级去重完成】待处理频道总数：{len(dedup_raw_channels)}")

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
        unique_urls = unique_list(urllist)
        std_channels[std_name] = unique_urls
        if len(urllist) > len(unique_urls):
            print(f"【频道二次去重】{std_name} 清除重复线路 {len(urllist) - len(unique_urls)} 条")
    print(f"【频道预处理结束】黑名单丢弃频道总数:{drop_black_channel_count}，进入测速频道总数:{len(std_channels)}")

    all_test_tasks_set = set()
    all_test_tasks = []
    for ch_name, urllist in std_channels.items():
        for link in urllist:
            task_key = (ch_name, link)
            if task_key not in all_test_tasks_set:
                all_test_tasks_set.add(task_key)
                all_test_tasks.append(task_key)
    del all_test_tasks_set

    total_task_num = len(all_test_tasks)
    print(f"\n【开始批量测速】总待检测链接数量：{total_task_num}")
    finished = 0
    channel_all_test_result = defaultdict(list)

    with ThreadPoolExecutor(max_workers=STREAM_EVAL_WORKERS) as pool:
        futures_map = {pool.submit(test_single_stream, link, url_black_list): (ch, link) for ch, link in all_test_tasks}
        for future in as_completed(futures_map):
            ch_name, url = futures_map[future]
            latency, url, ok, hit_url_black = future.result()
            finished += 1
            if hit_url_black or finished % 20 == 0:
                print(f"【测速进度 {finished}/{total_task_num}】已完成链接测速")
            if hit_url_black:
                print(f"  >> 【URL黑名单剔除广告链接】{url}")
                for s in url_belong_source.get(url, []):
                    source_statistics[s]["black"] += 1
                continue
            channel_all_test_result[ch_name].append((latency, url, ok))
            if not ok:
                auto_bad_url_set.add(url)
            for belong_src in url_belong_source.get(url, []):
                if ok:
                    if latency <= LATENCY_THRESHOLD:
                        source_statistics[belong_src]["good"] += 1
                    else:
                        source_statistics[belong_src]["fallback"] += 1
    print("【全部测速任务执行完毕】\n")

    append_bad_url_to_blacklist(auto_bad_url_set)

    template_queue = []
    channel_info = {}
    template_content = safe_read_file(TEMPLATE_OUTPUT_FILE)
    for line in template_content.splitlines():
        raw_line = line.strip()
        if not raw_line or "#genre#" in raw_line:
            continue
        parts = raw_line.split("|")
        if len(parts) >= 3:
            std_name, display_name, group_name = parts[0].strip(), parts[1].strip(), parts[2].strip()
        elif len(parts) == 2:
            std_name, display_name = parts[0].strip(), parts[1].strip()
            group_name = "默认分组"
        else:
            std_name = display_name = raw_line.strip()
            group_name = "默认分组"
        template_queue.append(std_name)
        channel_info[std_name] = (display_name, group_name)
    print(f"【模板读取完成】模板内待输出频道总数：{len(template_queue)}")

    matched_count = 0
    output_m3u_lines = ["#EXTM3U x-tvg-url=\"epg.xml.gz\""]
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

        show_name, group = channel_info[std_name]
        if valid_items:
            valid_items.sort(key=lambda x: x[0])
            output_links = [item[1] for item in valid_items[:MAX_LINK_PER_CHANNEL]]
            print(f"【正常输出】频道[{show_name}] 优质链接{len(output_links)}条")
            for link in output_links:
                output_m3u_lines.append(f'#EXTINF:-1 group-title="{group}",{show_name}')
                output_m3u_lines.append(link)
            matched_count += 1
        elif invalid_items:
            invalid_items.sort(key=lambda x: x[0])
            unique_fallback = unique_list([u for _, u in invalid_items])
            output_links = unique_fallback[:FALLBACK_MAX_LINK]
            print(f"【降级兜底】频道[{show_name}] 仅保留备选链接{len(output_links)}条")
            for link in output_links:
                output_m3u_lines.append(f'#EXTINF:-1 group-title="{group}",{show_name}')
                output_m3u_lines.append(link)

    with open(OUTPUT_TXT, "w", encoding="utf-8") as f:
        f.write("\n".join(output_m3u_lines))
    tvbox_text = m3u_to_tvbox_txt("\n".join(output_m3u_lines))
    with open(TV_BOX_OUTPUT, "w", encoding="utf-8") as wf:
        wf.write(tvbox_text)
    print(f"\n【额外生成完成】DIYP专用订阅文件：{TV_BOX_OUTPUT}")

    valid_channel_cnt = sum(1 for _, items in channel_all_test_result.items() if any(ok for _, _, ok in items))

    print("\n===================== 各订阅源链路检测统计报表 =====================")
    for source_url, stat in source_statistics.items():
        print(f"\n【订阅源】{source_url}")
        print(f"  ① 原始抓取总线路：{stat['total']} 条")
        print(f"  ② 广告黑名单过滤线路：{stat['black']} 条")
        print(f"  ③ 优质低延迟达标链接：{stat['good']} 条")
        print(f"  ④ 超时兜底可用链接：{stat['fallback']} 条")
        print(f"  ✅ 该源有效可用链接合计：{stat['good'] + stat['fallback']} 条")
    print("====================================================================\n")

    print(f"\n========== 全局执行汇总 ==========")
    print(f"存在可用连通链路的频道总数: {valid_channel_cnt}")
    print(f"template模板内成功输出频道总数: {matched_count}")
    print(f"标准M3U输出文件：{OUTPUT_TXT}")
    print(f"DIYP专用分组文件：{TV_BOX_OUTPUT}")
    print("==== IPTV分拣程序全部执行完毕 ====")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("脚本致命异常：")
        print(traceback.format_exc())
        # 弱化退出码，不触发bash -e中断
        sys.exit(0)
