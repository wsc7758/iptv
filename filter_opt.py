import os
import re
import time
import gc
import sys
import traceback
from collections import defaultdict, OrderedDict
from concurrent.futures import ThreadPoolExecutor, as_completed

# ====================== 全局配置常量 ======================
BASE_PATH = "config/"
BLACKLIST_FILE = os.path.join(BASE_PATH, "blacklist.txt")
# 输入：主脚本生成的iptv.txt（只读，全程不修改此文件）
INPUT_IPTV_FILE = "iptv.txt"
# 仅输出这一个文件
OUTPUT_FILE = "iptv1.txt"

# 今日分片GET测速参数（和老脚本HEAD测速区分开）
STREAM_REQ_TIMEOUT = 2.0
STREAM_EVAL_WORKERS = 2
MAX_LINK_PER_CHANNEL = 3  # 单个频道最多保留3条最优链路
LATENCY_THRESHOLD = 2.5
HTTP_RANGE_HEADERS = {"Range": "bytes=0-1023"}

# 全局正则预编译
REGEX_URL_SUFFIX_CLEAN = re.compile(r"\$.*$")
REGEX_GROUP_TITLE = re.compile(r'group-title="([^"]+)"')
REGEX_CCTV_PREFIX = re.compile(r"^CCTV-\d+")
# ======================================================================

import warnings
warnings.filterwarnings("ignore")
import requests
requests.packages.urllib3.disable_warnings()

# ====================== 通用工具函数 ======================
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

def load_url_blacklist():
    url_black_set = set()
    if not os.path.exists(BASE_PATH):
        os.makedirs(BASE_PATH)
    file_content = safe_read_file(BLACKLIST_FILE)
    for line in file_content.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("url*"):
            url = line[4:].strip()
            if url:
                url_black_set.add(url)
    print(f"【URL黑名单加载完成，共{len(url_black_set)}条拦截链接】")
    return url_black_set

def append_bad_url_to_blacklist(bad_url_set: set):
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
        print("【所有失效链接已存在黑名单，跳过写入】")
        return
    write_lines = ["# 自动测速检测失效链接 url|auto_bad"]
    write_lines.extend([f"url*{u}" for u in new_bad_urls])
    try:
        with open(BLACKLIST_FILE, "a", encoding="utf-8") as f:
            f.write("\n" + "\n".join(write_lines) + "\n")
        print(f"【黑名单写入完成，新增{len(new_bad_urls)}条失效链接】")
    except Exception as e:
        print(f"【警告：黑名单写入失败，本次不自动拉黑链接】err: {str(e)[:80]}")

def is_black_url(url, url_black_set):
    return url in url_black_set

# 今日分片GET测速核心函数（区别老脚本HEAD探测）
def test_single_stream(url, url_black_set):
    start = time.perf_counter()
    if is_black_url(url, url_black_set):
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

# 解析输入iptv.txt，严格保留原始分组、频道出现顺序，只读不修改源文件
def parse_keep_origin_order(input_text: str):
    channel_link_map = OrderedDict()
    channel_origin_order = []
    url_raw_line_map = dict()
    curr_group = "默认分组"
    curr_channel = ""
    temp_url_buffer = []

    for raw_line in input_text.splitlines():
        l_strip = raw_line.strip()
        if not l_strip:
            continue
        if l_strip.startswith("#EXTINF"):
            # 缓存上一个频道的链接数据
            if curr_channel and temp_url_buffer:
                key = (curr_group, curr_channel)
                if key not in channel_link_map:
                    channel_link_map[key] = []
                    channel_origin_order.append(key)
                channel_link_map[key].extend(temp_url_buffer)
                temp_url_buffer = []
            # 更新当前分组与频道名称
            g_match = REGEX_GROUP_TITLE.search(l_strip)
            if g_match:
                curr_group = g_match.group(1)
            if "," in l_strip:
                curr_channel = l_strip.split(",")[-1].strip()
        elif l_strip.startswith("http"):
            clean_url = clean_url_strip_suffix(l_strip)
            temp_url_buffer.append(clean_url)
            url_raw_line_map[clean_url] = raw_line
    # 收尾处理最后一组频道
    if curr_channel and temp_url_buffer:
        key = (curr_group, curr_channel)
        if key not in channel_link_map:
            channel_link_map[key] = []
            channel_origin_order.append(key)
        channel_link_map[key].extend(temp_url_buffer)
    return channel_link_map, channel_origin_order, url_raw_line_map

def main():
    print("========== 后置二次优化测速程序 启动 ==========")
    print(f"依赖前置脚本输出只读文件: {INPUT_IPTV_FILE}，全程不会修改该文件")
    if not os.path.exists(BASE_PATH):
        os.makedirs(BASE_PATH)
        print(f"【自动创建配置目录】{BASE_PATH}")

    url_black_set = load_url_blacklist()
    auto_bad_url_set = set()

    # 读取前置脚本生成的iptv.txt，仅读取，无写入操作
    raw_text = safe_read_file(INPUT_IPTV_FILE)
    if not raw_text:
        print(f"【致命错误】前置文件 {INPUT_IPTV_FILE} 不存在，前置分拣脚本未执行完成，程序退出")
        sys.exit(1)
    channel_link_map, channel_origin_order, url_raw_line_map = parse_keep_origin_order(raw_text)
    print(f"【读取前置源完成，原始频道顺序锁定，待测频道：{len(channel_origin_order)} 个】")

    # 收集全部待测链接
    all_test_tasks = []
    for (g, ch), url_list in channel_link_map.items():
        unique_urls = unique_list(url_list)
        for url in unique_urls:
            all_test_tasks.append((g, ch, url))
    total_test_url = len(all_test_tasks)
    print(f"【二次测速总链接数量：{total_test_url} 条】")

    # 批量分片测速
    finished_count = 0
    test_result_map = defaultdict(list)
    with ThreadPoolExecutor(max_workers=STREAM_EVAL_WORKERS) as pool:
        task_futures = {}
        for g_name, ch_name, clean_url in all_test_tasks:
            task_key = (g_name, ch_name)
            task_futures[pool.submit(test_single_stream, clean_url, url_black_set)] = (task_key, clean_url)

        for future in as_completed(task_futures):
            task_key, clean_url = task_futures[future]
            latency, url, ok, hit_black = future.result()
            finished_count += 1
            if finished_count % 30 == 0:
                print(f"【测速进度 {finished_count}/{total_test_url}】")

            if hit_black:
                print(f"【黑名单拦截链接】{clean_url}")
                continue
            test_result_map[task_key].append((latency, clean_url, ok))
            if not ok:
                auto_bad_url_set.add(clean_url)
    print("【二次测速全部完成】")
    append_bad_url_to_blacklist(auto_bad_url_set)

    # 严格按照原始iptv.txt顺序生成输出内容，不打乱原有频道排序
    output_m3u_lines = ["#EXTM3U x-tvg-url=\"epg.xml.gz\""]
    valid_channel_total = 0
    for task_key in channel_origin_order:
        group_name, ch_name = task_key
        res_list = test_result_map.get(task_key, [])
        if not res_list:
            continue
        good_links = []
        fallback_links = []
        for lat, clean_url, ok in res_list:
            if ok:
                if lat <= LATENCY_THRESHOLD:
                    good_links.append((lat, clean_url))
                else:
                    fallback_links.append((lat, clean_url))
        # 延迟升序排序
        good_links.sort(key=lambda x: x[0])
        fallback_links.sort(key=lambda x: x[0])
        sorted_all = good_links + fallback_links
        if not sorted_all:
            continue
        valid_channel_total += 1
        keep_urls = [item[1] for item in sorted_all[:MAX_LINK_PER_CHANNEL]]
        print(f"【输出频道】{group_name} - {ch_name} 保留{len(keep_urls)}条最优线路")
        # 还原原始#EXTINF分组频道行
        output_m3u_lines.append(f'#EXTINF:-1 group-title="{group_name}",{ch_name}')
        # 写入原始完整URL文本，完全复刻原文件格式
        for u in keep_urls:
            output_m3u_lines.append(url_raw_line_map[u])

    # 独立输出唯一文件 iptv1.txt，不再生成tv.txt
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write("\n".join(output_m3u_lines))
    print(f"\n【二次优化成品已输出至：{OUTPUT_FILE}】严格沿用iptv.txt原始分组、频道顺序，原iptv.txt无任何修改")

    print(f"\n========== 后置优化程序执行汇总 ==========")
    print(f"测速后存在可用链路的频道总数：{valid_channel_total}")
    print(f"单频道最大保留最优链路数量：{MAX_LINK_PER_CHANNEL}")
    print(f"运行逻辑：只读iptv.txt，仅独立输出iptv1.txt，两套程序互不干扰")
    print("==== 后置优化程序全部执行完毕 ====")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("后置优化程序捕获全局致命异常：")
        print(traceback.format_exc())
        sys.exit(0)
