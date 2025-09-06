import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin
import os
import json
import re
from datetime import datetime

# Constants
MAX_URLS = 500
MAX_PREVNEXT_URLS = 50
MAX_API_PAGES = 1
DEFAULT_API_URL_PATTERN = "https://{domain}/wp-json/wp/v2/product?per_page=100&page={page}&orderby=date&order=desc"
HEADERS = {"User-Agent": "Mozilla/50.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"}
REPO_URL_PATTERN = "https://raw.githubusercontent.com/chanktb/product-crawler/main/{domain}.txt"
IMAGE_REPO_URL_PATTERN = "https://raw.githubusercontent.com/ktbhub/image-crawler/main/{domain}.txt"
STOP_URLS_FILE = "stop_urls.txt"
STOP_URLS_COUNT = 10  # Số lượng URL sản phẩm được lưu làm điểm dừng

# ----------------------------------------------------------------------------------------------------------------------
# Core Functions
# ----------------------------------------------------------------------------------------------------------------------

def load_config():
    """Tải cấu hình từ một tệp config.json duy nhất."""
    try:
        with open('config.json', 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        print("Lỗi: Không tìm thấy tệp config.json!")
        return []

def load_stop_urls():
    """Tải danh sách URL dừng từ stop_urls.txt."""
    try:
        with open(STOP_URLS_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}

def save_stop_urls(stop_urls):
    """Lưu danh sách URL dừng vào stop_urls.txt."""
    with open(STOP_URLS_FILE, 'w', encoding='utf-8') as f:
        json.dump(stop_urls, f, indent=2)

def check_url_exists(url):
    """Kiểm tra xem một URL có tồn tại không bằng cách gửi yêu cầu HEAD."""
    try:
        r = requests.head(url, headers=HEADERS, timeout=10)
        return r.status_code == 200
    except requests.exceptions.RequestException:
        return False

def apply_replacements(image_url, replacements):
    """Áp dụng logic thay thế URL hình ảnh."""
    final_img_url = image_url
    if replacements:
        for original, replacement_list in replacements.items():
            if original in image_url:
                for replacement in replacement_list:
                    new_url = image_url.replace(original, replacement)
                    final_img_url = new_url
                    break
            if final_img_url != image_url:
                break
    return final_img_url

def apply_fallback_logic(image_url, url_data):
    """
    Áp dụng logic thay thế đặc biệt (cut_filename_prefix)
    và kiểm tra sự tồn tại bằng HEAD request.
    """
    fallback_rules = url_data.get('fallback_rules', {})

    if not fallback_rules or fallback_rules.get('type') != 'cut_filename_prefix':
        return image_url

    parsed_url = urlparse(image_url)
    if parsed_url.netloc != fallback_rules.get('domain'):
        return image_url

    path_parts = parsed_url.path.split('/')
    filename = path_parts[-1]
    prefix_length = fallback_rules.get('prefix_length', 0)

    # Check if the filename has the expected format before cutting
    if len(filename) > prefix_length and filename[prefix_length - 1] == '-':
        new_filename = filename[prefix_length:]
        new_path = '/'.join(path_parts[:-1] + [new_filename])
        modified_url = parsed_url._replace(path=new_path).geturl()

        print(f"[{url_data['url']}] Checking fallback URL: {modified_url}")
        if check_url_exists(modified_url):
            print(f"[{url_data['url']}] ✅ Found valid URL using fallback logic for original: {image_url}")
            return modified_url
        else:
            print(f"[{url_data['url']}] ❌ Fallback URL not found. Using original.")
            
    return image_url

# ----------------------------------------------------------------------------------------------------------------------
# Crawl Functions
# ----------------------------------------------------------------------------------------------------------------------

def find_best_image_url(soup, url_data):
    """Tìm URL hình ảnh tốt nhất dựa trên logic ưu tiên."""
    replacements = url_data.get('replacements', {})
    selector = url_data.get('selector')

    # 1. Tìm kiếm URL có chứa chuỗi thay thế trong phạm vi selector (nếu có)
    if selector:
        image_tags_to_search = soup.select(selector)
    else:
        image_tags_to_search = soup.find_all('img')

    if replacements:
        for img_tag in image_tags_to_search:
            img_url = img_tag.get('src') or img_tag.get('data-src') or img_tag.get('data-lazy-src')
            if img_url:
                for original in replacements.keys():
                    if original in img_url:
                        print(f"Found prioritized URL in HTML: {img_url}")
                        final_url = apply_replacements(img_url, replacements)
                        return final_url
    
    # 2. Fallback sang og:image
    og_image_tag = soup.find('meta', property='og:image')
    if og_image_tag:
        img_url = og_image_tag.get('content')
        if img_url:
            print(f"Using fallback og:image URL: {img_url}")
            return img_url
            
    # 3. Fallback sang img tag thông thường
    if not selector and not replacements:
        for img_tag in soup.find_all('img'):
            img_url = img_tag.get('src') or img_tag.get('data-src') or img_tag.get('data-lazy-src')
            if img_url:
                print(f"Using standard img tag URL: {img_url}")
                return img_url
            
    return None

def fetch_image_urls_from_web(url_data):
    """Tải và phân tích URL hình ảnh trực tiếp từ trang web."""
    all_image_urls = []
    try:
        r = requests.get(url_data['url'], headers=HEADERS, timeout=30)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
    except requests.exceptions.RequestException as e:
        print(f"Lỗi khi truy cập {url_data['url']}: {e}")
        return []

    best_url = find_best_image_url(soup, url_data)
    if best_url:
        final_url = apply_replacements(best_url, url_data.get('replacements', {}))
        final_url = apply_fallback_logic(final_url, url_data)
        all_image_urls.append(final_url)
        
    return all_image_urls

def fetch_image_urls_from_api(url_data):
    """Tải và phân tích URL hình ảnh từ API."""
    all_image_urls = []
    page = 1
    domain = urlparse(url_data['url']).netloc
    
    while page <= MAX_API_PAGES:
        api_url = DEFAULT_API_URL_PATTERN.format(domain=domain, page=page)
        print(f"Fetching from API: {api_url}")
        try:
            r = requests.get(api_url, headers=HEADERS, timeout=30)
            r.raise_for_status()
            data = r.json()
            if not data:
                break
            
            for item in data:
                img_url = None
                
                if 'yoast_head_json' in item and 'og_image' in item['yoast_head_json'] and len(item['yoast_head_json']['og_image']) > 0:
                    img_url = item['yoast_head_json']['og_image'][0]['url']
                
                if not img_url and 'content' in item and 'rendered' in item['content']:
                    soup = BeautifulSoup(item['content']['rendered'], 'html.parser')
                    img_tag = soup.find('img')
                    if img_tag and img_tag.get('src'):
                        img_url = img_tag.get('src')
                
                if img_url:
                    if img_url.startswith('http://'):
                        img_url = img_url.replace('http://', 'https://')
                    
                    final_img_url = apply_replacements(img_url, url_data.get('replacements', {}))
                    final_img_url = apply_fallback_logic(final_img_url, url_data)
                    
                    if final_img_url not in all_image_urls:
                        all_image_urls.append(final_img_url)
            
            page += 1
        except requests.exceptions.RequestException as e:
            print(f"Lỗi khi truy cập API {api_url}: {e}")
            break
            
    return all_image_urls

def fetch_image_urls_from_prevnext(url_data):
    """Crawl sản phẩm theo chuỗi next/prev với cơ chế khôi phục."""
    all_image_urls = []
    domain = urlparse(url_data['url']).netloc

    try:
        r = requests.get(url_data['url'], headers=HEADERS, timeout=30)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        first_product_tag = soup.select_one(url_data['first_product_selector'])
        if not first_product_tag:
            print(f"Không tìm thấy sản phẩm đầu tiên trên {url_data['url']}")
            return []
        current_product_url = urljoin(url_data['url'], first_product_tag.get('href'))
        last_successful_product_url = None
    except requests.exceptions.RequestException as e:
        print(f"Lỗi khi truy cập trang chủ {url_data['url']}: {e}")
        return []

    count = 0
    while count < MAX_PREVNEXT_URLS:
        try:
            r = requests.get(current_product_url, headers=HEADERS, timeout=30)
            r.raise_for_status()
            soup = BeautifulSoup(r.text, "html.parser")

            best_url = find_best_image_url(soup, url_data)
            if best_url:
                final_img_url = apply_replacements(best_url, url_data.get('replacements', {}))
                final_img_url = apply_fallback_logic(final_img_url, url_data)
                
                if final_img_url not in all_image_urls:
                    all_image_urls.append(final_img_url)
            
            last_successful_product_url = current_product_url
            
            next_product_tag = soup.select_one(url_data['next_product_selector'])
            if not next_product_tag or not next_product_tag.get('href'):
                print("Không tìm thấy sản phẩm tiếp theo, kết thúc.")
                break
            
            current_product_url = urljoin(current_product_url, next_product_tag.get('href'))
            count += 1
        except requests.exceptions.RequestException as e:
            print(f"Lỗi khi truy cập {current_product_url}: {e}")
            print(f"URL thành công gần nhất: {last_successful_product_url}")
            
            repo_file_url = REPO_URL_PATTERN.format(domain=domain)
            try:
                repo_file = requests.get(repo_file_url, headers=HEADERS, timeout=30)
                if repo_file.status_code == 200:
                    repo_urls = [line.strip() for line in repo_file.text.splitlines() if line.strip()]
                    if last_successful_product_url and last_successful_product_url in repo_urls:
                        last_crawled_index = repo_urls.index(last_successful_product_url)
                        next_urls_to_check = repo_urls[last_crawled_index + 1 : last_crawled_index + 4]
                        
                        found_next_valid = False
                        for next_url in next_urls_to_check:
                            if check_url_exists(next_url):
                                current_product_url = next_url
                                print(f"Phục hồi crawl từ URL: {current_product_url}")
                                found_next_valid = True
                                break
                        
                        if found_next_valid:
                            continue
                        else:
                            print("Không thể tìm thấy URL hợp lệ trong repo, kết thúc.")
                            break
                    else:
                        print("URL gần nhất không có trong repo, kết thúc.")
                        break
                else:
                    print(f"Không thể truy cập repo {repo_file_url}, kết thúc.")
                    break
            except requests.exceptions.RequestException as e:
                print(f"Lỗi khi truy cập repo: {e}, kết thúc.")
                break

    return all_image_urls

def fetch_image_urls_from_product_list(url_data, stop_urls_list):
    """Tải danh sách URL sản phẩm từ repo và crawl từng trang để lấy ảnh."""
    all_image_urls = []
    domain = urlparse(url_data['url']).netloc
    repo_file_url = REPO_URL_PATTERN.format(domain=domain)
    
    # Lấy danh sách URL sản phẩm từ repo
    try:
        r = requests.get(repo_file_url, headers=HEADERS, timeout=30)
        r.raise_for_status()
        product_urls = [line.strip() for line in r.text.splitlines() if line.strip()]
    except requests.exceptions.RequestException as e:
        print(f"Lỗi khi truy cập repo sản phẩm: {e}. Bỏ qua domain này.")
        return []
    
    urls_to_crawl = []
    
    # Duyệt qua danh sách sản phẩm để tìm điểm dừng
    if stop_urls_list:
        found_stop_point = False
        for product_url in product_urls:
            if product_url in stop_urls_list:
                print(f"Đã tìm thấy URL dừng: {product_url}, kết thúc tìm kiếm sản phẩm mới.")
                found_stop_point = True
                break
            urls_to_crawl.append(product_url)
        
        if not found_stop_point:
            print(f"Không tìm thấy URL dừng cho {domain}. Crawl toàn bộ danh sách.")
            urls_to_crawl = product_urls
    else:
        urls_to_crawl = product_urls

    # Xử lý các URL sản phẩm mới
    for product_url in urls_to_crawl:
        if len(all_image_urls) >= MAX_PREVNEXT_URLS:
            print("Đạt giới hạn URL, kết thúc crawl.")
            break

        try:
            r = requests.get(product_url, headers=HEADERS, timeout=30)
            r.raise_for_status()
            soup = BeautifulSoup(r.text, "html.parser")
            
            best_url = find_best_image_url(soup, url_data)
            if best_url:
                final_img_url = apply_replacements(best_url, url_data.get('replacements', {}))
                final_img_url = apply_fallback_logic(final_img_url, url_data)
                
                if final_img_url not in all_image_urls:
                    all_image_urls.append(final_img_url)
        except requests.exceptions.RequestException as e:
            print(f"Lỗi khi truy cập URL sản phẩm {product_url}: {e}. Bỏ qua.")
            continue

    return all_image_urls

# ----------------------------------------------------------------------------------------------------------------------
# Main Execution
# ----------------------------------------------------------------------------------------------------------------------

def save_urls(domain, new_urls):
    """Lưu các URL mới vào đầu tệp của domain tương ứng."""
    filename = f"{domain}.txt"

    try:
        with open(filename, "r", encoding="utf-8") as f:
            existing_urls = [line.strip() for line in f.readlines() if line.strip()]
    except FileNotFoundError:
        existing_urls = []

    unique_new_urls = [u for u in new_urls if u not in existing_urls]
    all_urls = unique_new_urls + existing_urls
    all_urls = all_urls[:MAX_URLS]
    
    with open(filename, "w", encoding="utf-8") as f:
        f.write("\n".join(all_urls))

    print(f"[{domain}] Added {len(unique_new_urls)} new URLs. Total: {len(all_urls)}")
    return len(unique_new_urls), len(all_urls)

if __name__ == "__main__":
    configs = load_config()
    if not configs:
        exit(1)

    urls_summary = {}
    
    # Tải stop_urls một lần duy nhất
    stop_urls_data = load_stop_urls()
    
    for url_data in configs:
        domain = urlparse(url_data['url']).netloc
        
        try:
            with open(f"{domain}.txt", "r", encoding="utf-8") as f:
                existing_urls = [line.strip() for line in f if line.strip()]
        except FileNotFoundError:
            existing_urls = []
        
        source_type = url_data.get('source_type')
        if source_type == 'web':
            image_urls = fetch_image_urls_from_web(url_data)
        elif source_type == 'api':
            image_urls = fetch_image_urls_from_api(url_data)
        elif source_type == 'prevnext':
            image_urls = fetch_image_urls_from_prevnext(url_data)
        elif source_type == 'product-list':
            domain_stop_urls_list = stop_urls_data.get(domain, [])
            image_urls = fetch_image_urls_from_product_list(url_data, domain_stop_urls_list)
            
            # Sau khi crawl product-list, cập nhật danh sách stop_urls mới
            product_repo_url = REPO_URL_PATTERN.format(domain=domain)
            try:
                r = requests.get(product_repo_url, headers=HEADERS, timeout=30)
                r.raise_for_status()
                product_urls_from_repo = [line.strip() for line in r.text.splitlines() if line.strip()]
                
                new_stop_urls = product_urls_from_repo[:STOP_URLS_COUNT]
                stop_urls_data[domain] = new_stop_urls
            except requests.exceptions.RequestException as e:
                print(f"Lỗi khi cập nhật stop_urls cho domain {domain}: {e}")
        else:
            print(f"Lỗi: Không xác định được source_type cho domain {domain}. Bỏ qua.")
            continue
            
        print(f"[{domain}] Found {len(image_urls)} potential image URLs.")
        new_urls_count, total_urls_count = save_urls(domain, image_urls)
        urls_summary[domain] = {'new_count': new_urls_count, 'total_count': total_urls_count}

    # Lưu lại file stop_urls.txt sau khi tất cả các domain đã được xử lý
    save_stop_urls(stop_urls_data)

    with open("crawl-log.txt", "w", encoding="utf-8") as f:
        f.write("-Kết quả crawl-\n")
        f.write(f"Thời gian: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        for domain, counts in urls_summary.items():
            f.write(f"{domain}: {counts['new_count']} new URLs added. Total {counts['total_count']} URLs.\n")

    print("\n--- Summary saved to crawl-log.txt ---")
