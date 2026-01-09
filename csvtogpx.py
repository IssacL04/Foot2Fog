import pandas as pd
import os
import glob
import datetime
from xml.etree.ElementTree import Element, SubElement, ElementTree

# ================= Setup =================
INPUT_FOLDER = 'input'
OUTPUT_FOLDER = 'output'
MAX_GAP_SECONDS = 300  # Over 300 seconds: Regarded as disconnected and start a new line
INTERPOLATION_STEP = 1 # 1 interpolation per second
# ===========================================

def ensure_folders_exist():
    if not os.path.exists(INPUT_FOLDER):
        os.makedirs(INPUT_FOLDER)
        print(f"已创建输入文件夹: ./{INPUT_FOLDER}/ (请将CSV文件放入此处)")
    if not os.path.exists(OUTPUT_FOLDER):
        os.makedirs(OUTPUT_FOLDER)
        print(f"已创建输出文件夹: ./{OUTPUT_FOLDER}/")

def standardize_dataframe(df, filename):
    """
    智能识别 CSV 格式并标准化列名;返回: (标准化后的df, 格式名称) 或 (None, None)
    """
    df.columns = df.columns.str.strip()
    cols = df.columns.tolist()
    
    # --- Type A: Variflight ---
    # Time, Latitude, Longitude, Height 
    if 'Time' in cols and 'Latitude' in cols and 'Longitude' in cols:
        print(f"识别为 [Variflight/飞常准] 格式")
        df = df.rename(columns={
            'Time': 'timestamp',
            'Latitude': 'lat',
            'Longitude': 'lon',
            'Height': 'ele'
        })
        df['timestamp'] = pd.to_numeric(df['timestamp'], errors='coerce')
        return df, "Variflight"

    # --- Type B: Footprint ---
    # dataTime, latitude, longitude 
    elif 'dataTime' in cols and 'latitude' in cols and 'longitude' in cols:
        print(f"识别为 [Footprint/一生足迹] 格式")
        df = df.rename(columns={
            'dataTime': 'timestamp',
            'latitude': 'lat',
            'longitude': 'lon',
            'altitude': 'ele'  
        })
        df['timestamp'] = pd.to_numeric(df['timestamp'], errors='coerce')
        return df, "Footprint"

    else:
        print(f"无法识别文件 {filename} 的列格式。")
        print(f"当前列名: {cols}")
        return None, None

def process_and_generate_gpx(df, output_path):
    if 'ele' not in df.columns:
        df['ele'] = 0 
    
    df['dt_object'] = pd.to_datetime(df['timestamp'], unit='s')
    df = df.sort_values(by='dt_object').reset_index(drop=True)
    df = df.dropna(subset=['lat', 'lon', 'timestamp']) 

    gpx = Element('gpx', version="1.1", creator="AutoBatchConverter", xmlns="http://www.topografix.com/GPX/1/1")
    trk = SubElement(gpx, 'trk')
    SubElement(trk, 'name').text = os.path.basename(output_path).replace('.gpx', '')
    trkseg = SubElement(trk, 'trkseg')

    count_generated = 0
    
    for i in range(len(df) - 1):
        curr = df.iloc[i]
        next_p = df.iloc[i+1]

        t1, t2 = curr['dt_object'], next_p['dt_object']
        lat1, lon1, ele1 = curr['lat'], curr['lon'], curr['ele']
        lat2, lon2, ele2 = next_p['lat'], next_p['lon'], next_p['ele']

        time_diff = (t2 - t1).total_seconds()

        if time_diff > MAX_GAP_SECONDS:
            _write_pt(trkseg, lat1, lon1, ele1, t1)
            trkseg = SubElement(trk, 'trkseg')
            continue
        
        if time_diff <= INTERPOLATION_STEP:
            _write_pt(trkseg, lat1, lon1, ele1, t1)
            count_generated += 1
            continue

        steps = int(time_diff / INTERPOLATION_STEP)
        for step in range(steps):
            fraction = step / steps
            i_lat = lat1 + (lat2 - lat1) * fraction
            i_lon = lon1 + (lon2 - lon1) * fraction
            i_ele = ele1 + (ele2 - ele1) * fraction
            i_time = t1 + datetime.timedelta(seconds=step)
            
            _write_pt(trkseg, i_lat, i_lon, i_ele, i_time)
            count_generated += 1

    last = df.iloc[-1]
    _write_pt(trkseg, last['lat'], last['lon'], last['ele'], last['dt_object'])

    ElementTree(gpx).write(output_path, encoding='utf-8', xml_declaration=True)
    return count_generated

def _write_pt(trkseg, lat, lon, ele, time_obj):
    pt = SubElement(trkseg, 'trkpt', lat=f"{lat:.6f}", lon=f"{lon:.6f}")
    SubElement(pt, 'ele').text = f"{ele:.2f}"
    SubElement(pt, 'time').text = time_obj.strftime('%Y-%m-%dT%H:%M:%SZ')

def main():
    print("Application Starting……")
    ensure_folders_exist()

    csv_files = glob.glob(os.path.join(INPUT_FOLDER, "*.csv"))
    
    if not csv_files:
        print(f"⚠️  {INPUT_FOLDER} 文件夹为空！请放入 CSV 文件后重新运行。")
        return

    print(f"发现 {len(csv_files)} 个 CSV 文件，开始处理...\n")

    for file_path in csv_files:
        filename = os.path.basename(file_path)
        print(f"正在处理: {filename}")
        
        try:
            df = pd.read_csv(file_path)
            
            std_df, fmt_type = standardize_dataframe(df, filename)
            
            if std_df is not None:
                output_filename = os.path.splitext(filename)[0] + ".gpx"
                output_path = os.path.join(OUTPUT_FOLDER, output_filename)
                
                pts_count = process_and_generate_gpx(std_df, output_path)
                print(f"转换成功! ({fmt_type}) -> 生成 {pts_count} 个轨迹点")
                print(f"保存至: {output_path}")
            
        except Exception as e:
            print(f"处理出错: {str(e)}")
        
        print("-" * 40)

    print("\n所有任务处理完成")

if __name__ == "__main__":
    main()