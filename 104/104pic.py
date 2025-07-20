# 3_table_extractor.py

import os
import re
import cv2
import pandas as pd
import pytesseract

# 如果你沒有把 tesseract 加到系統 PATH，請解除下面註解並設定你的安裝路徑：
pytesseract.pytesseract.tesseract_cmd = r'C:\Program Files\TesseractOCR\tesseract.exe'

# --- 使用者設定區 ---
SCREENSHOT_DIR = "C:\\Class\\Houseprice\\104_ocr_screenshots"   # 放截圖的資料夾
OUTPUT_DIR      = "output_csvs"
ROI_CONFIG      = "roi_config.txt"   # 存放手動選取的 crop 參數

os.makedirs(OUTPUT_DIR, exist_ok=True)

def get_roi(gray):
    """
    如果已有 roi_config.txt，就讀取裡面的 x1,y1,x2,y2
    否則跳出互動式視窗讓你選取一次，然後存檔並直接 exit(0)
    """
    if os.path.exists(ROI_CONFIG):
        x1,y1,x2,y2 = map(int, open(ROI_CONFIG).read().split(","))
        return x1,y1,x2,y2

    # 互動框選一次
    r = cv2.selectROI("請框選表格區域，選好後按 Enter 或 空白鍵", gray, False, False)
    cv2.destroyAllWindows()
    x,y,w,h = map(int, r)
    if w==0 or h==0:
        raise RuntimeError("沒有選到任何區域")
    x1,y1,x2,y2 = x, y, x+w, y+h

    # 存檔，下次自動載入
    with open(ROI_CONFIG, "w") as f:
        f.write(f"{x1},{y1},{x2},{y2}")
    print(f"已儲存 ROI 參數到 {ROI_CONFIG}: {x1},{y1},{x2},{y2}")
    print("請再執行一次腳本，將開始批次處理所有圖片。")
    exit(0)

def extract_table_data_from_image(image_path, code):
    # 1. 讀圖 → 灰階
    img = cv2.imread(image_path)
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)

    # 2. 取得 ROI（自動或互動），裁切並放大
    x1,y1,x2,y2 = get_roi(gray)
    roi = gray[y1:y2, x1:x2]
    roi = cv2.resize(roi, None, fx=2.0, fy=2.0, interpolation=cv2.INTER_CUBIC)

    # 3. 二值化 + 去雜訊
    bw = cv2.adaptiveThreshold(
        roi, 255,
        cv2.ADAPTIVE_THRESH_GAUSSIAN_C,
        cv2.THRESH_BINARY,
        25, 15
    )
    clean = cv2.morphologyEx(bw,
                             cv2.MORPH_CLOSE,
                             cv2.getStructuringElement(cv2.MORPH_RECT, (3,3)))

    # （可選）存 debug 圖檢查
    cv2.imwrite(f"debug_crop_{code}.png", clean)

    # 4. OCR：繁中+英文，--psm 6
    raw = pytesseract.image_to_string(
        clean,
        lang='chi_tra+eng',
        config='--psm 6'
    )

    # 5. 把簡體「万」換成繁體「萬」
    text = raw.replace('万', '萬')

    # 6. 正規擷取
    pattern = re.compile(
        r'^(?P<職位>[\u4e00-\u9fa5A-Za-z0-9 ]+?)\s*'
        r'(?P<P25>\d+\.?\d*萬)\s*[~～]\s*(?P<P75>\d+\.?\d*萬)\s+'
        r'(?P<均薪>\d+\.?\d*萬)\s+'
        r'(?P<職缺>\d+)$'
    )

    rows = []
    for line in text.splitlines():
        line = line.strip()
        if not line: continue
        m = pattern.search(line)
        if m:
            d = m.groupdict()
            rows.append({
                '職位':   d['職位'].strip(),
                '月薪P25': d['P25'],
                '月薪P75': d['P75'],
                '月均薪': d['均薪'],
                '職缺數': d['職缺']
            })
    return rows

def main():
    files = sorted(f for f in os.listdir(SCREENSHOT_DIR)
                   if f.lower().endswith(('.png','.jpg','.jpeg')))
    if not files:
        print(f"錯誤：資料夾 '{SCREENSHOT_DIR}' 中沒有圖片！")
        return

    for idx, fname in enumerate(files, 1):
        code = os.path.splitext(fname)[0]
        path = os.path.join(SCREENSHOT_DIR, fname)
        print(f"[{idx}/{len(files)}] 處理：{fname}")

        rows = extract_table_data_from_image(path, code)
        if not rows:
            print("  → 警告：未擷取到任何資料。")
            continue

        # 加上職務代碼
        for r in rows:
            r['職務代碼'] = code

        df = pd.DataFrame(rows, columns=[
            '職務代碼','職位','月薪P25','月薪P75','月均薪','職缺數'
        ])
        out_csv = os.path.join(OUTPUT_DIR, f"{code}.csv")
        df.to_csv(out_csv, index=False, encoding='utf-8-sig')
        print(f"  → 已輸出：{out_csv}")

    print("全部完成！")

if __name__ == "__main__":
    main()