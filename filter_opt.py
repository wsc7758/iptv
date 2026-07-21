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
# 输入输出统一为tv.txt：读取原文件，筛选后覆盖写入
INPUT_FILE = "tv.txt"
OUTPUT_FILE = "tv.txt"

# 分片GET精细测速参数
STREAM_REQ_TIMEOUT = 2.0
STREAM_EVAL_WORKERS = 2
MAX_LINK_PER_CHANNEL = 3  # 单个频道最多保留3条最优链路
LATENCY_THRESHOLD = 2.5
# 改为512KB分片，真实模拟播放加载，测速精度大幅提升
HTTP_RANGE_HEADERS = {"Range": "bytes=0-524287"}

# 正则预编译
REGEX_URL_SUFFIX_CLEAN = re.compile(r"\$.*$")
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

# 分片GET测速核心函数
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

# 解析tv.txt标准DIYP格式，保留原始分组顺序、频道原始行文本
def parse_tv_format(input_text: str):
    group_order = []        # 记录分组出现原始顺序
    group_channel_map = OrderedDict()
    channel_raw_line = dict() # 干净url → 原始"频道名,url"整行
    curr_group = ""

    for raw_line in input_text.splitlines():
        line_strip = raw_line.strip()
        if not line_strip:
            continue
        # 识别分组标记行
        if "#genre#" in line_strip:
            curr_group = line_strip.split(",")[0].strip()
            if curr_group not in group_channel_map:
                group_channel_map[curr_group] = OrderedDict()
                group_order.append(curr_group)
            continue
        # 频道+URL行
        if "," in line_strip:
            ch_name, raw_url = line_strip.split(",", 1)
            ch_name = ch_name.strip()
            raw_url = raw_url.strip()
            clean_url = clean_url_strip_suffix(raw_url)
            if curr_group and ch_name and clean_url.startswith("http"):
                if ch_name not in group_channel_map[curr_group]:
                    group_channel_map[curr_group][ch_name] = []
                group_channel_map[curr_group][ch_name].append(clean_url)
                channel_raw_line[clean_url] = line_strip
    return group_order, group_channel_map, channel_raw_line

def main():
    print("========== 后置TV.TXT精细测速筛选程序 启动 ==========")
    print(f"读取源文件: {INPUT_FILE}，筛选完成后直接覆盖写入{OUTPUT_FILE}")
    if not os.path.exists(BASE_PATH):
        os.makedirs(BASE_PATH)
        print(f"【自动创建配置目录】{BASE_PATH}")

    url_black_set = load_url_blacklist()
    auto_bad_url_set = set()

    # 只读原始tv.txt，全程不修改原文件，仅读取数据
    raw_text = safe_read_file(INPUT_FILE)
    if not raw_text:
        print(f"【致命错误】文件 {INPUT_FILE} 不存在，程序退出")
        sys.exit(1)
    group_order, group_channel_map, channel_raw_line = parse_tv_format(raw_text)
    print(f"【读取完成，原始分组顺序锁定，共{len(group_order)}个分组】")

    # 收集全部待测链接
    all_test_tasks = []
    task_index_map = dict() # (分组,频道) 绑定url列表
    for g_name in group_order:
        ch_dict = group_channel_map[g_name]
        for ch_name, url_list in ch_dict.items():
            unique_urls = unique_list(url_list)
            for url in unique_urls:
                all_test_tasks.append((g_name, ch_name, url))
                task_index_map[(g_name, ch_name)] = url_list
    total_test_url = len(all_test_tasks)
    print(f"【待测速总链接数量：{total_test_url} 条】")

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
    print("【全部测速任务执行完毕】")
    append_bad_url_to_blacklist(auto_bad_url_set)

    # 严格按原始分组、频道顺序重构输出文本
    output_lines = []
    valid_channel_total = 0
    for group_name in group_order:
        # 写入分组标记行
        output_lines.append(f"{group_name},#genre#")
        ch_dict = group_channel_map[group_name]
        for ch_name in ch_dict.keys():
            task_key = (group_name, ch_name)
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
            # 延迟从小到大排序
            good_links.sort(key=lambda x: x[0])
            fallback_links.sort(key=lambda x: x[0])
            sorted_all = good_links + fallback_links
            if not sorted_all:
                continue
            valid_channel_total += 1
            # 截取最优前5条链路
            keep_urls = [item[1] for item in sorted_all[:MAX_LINK_PER_CHANNEL]]
            print(f"【输出频道】{group_name} - {ch_name} 保留{len(keep_urls)}条最优线路")
            # 写入原始频道行文本
            for u in keep_urls:
                output_lines.append(channel_raw_line[u])

    # 过滤空行、标准LF换行，覆盖写入tv.txt
    final_write_lines = [line.strip() for line in output_lines if line.strip()]
    with open(OUTPUT_FILE, "w", encoding="utf-8", newline="\n") as f:
        f.write("\n".join(final_write_lines))
    print(f"\n【筛选完成，已覆盖更新{OUTPUT_FILE}】严格保留原分组、频道顺序")

    print(f"\n========== 程序执行汇总 ==========")
    print(f"测速后存在可用链路的频道总数：{valid_channel_total}")
    print(f"单频道最大保留最优链路数量：{MAX_LINK_PER_CHANNEL}")
    print(f"流程：只读tv.txt测速筛选 → 覆盖输出回tv.txt，无多余文件")
    print("==== 后置筛选程序全部执行完毕 ====")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("后置筛选程序捕获全局致命异常：")
        print(traceback.format_exc())
        sys.exit(0)
