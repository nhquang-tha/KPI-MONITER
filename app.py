import os
import pandas as pd
import json
import gc
import re
import zipfile
import unicodedata
import random
import math
import requests
import urllib.parse
from io import BytesIO, StringIO
from datetime import datetime, timedelta
from flask import Flask, render_template_string, request, redirect, url_for, flash, send_file, Response, stream_with_context, jsonify
from flask_sqlalchemy import SQLAlchemy
from flask_login import LoginManager, UserMixin, login_user, login_required, logout_user, current_user
from werkzeug.security import generate_password_hash, check_password_hash
from sqlalchemy import text, func, inspect, or_, and_
from itertools import zip_longest
from collections import defaultdict

# ==============================================================================
# 1. APP CONFIGURATION & DATABASE SETUP
# ==============================================================================

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'bi_mat_khong_the_bat_mi')
app.config['SQLALCHEMY_DATABASE_URI'] = os.environ.get('DATABASE_URL', 'sqlite:///local.db')
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['SQLALCHEMY_ENGINE_OPTIONS'] = {'pool_recycle': 280, 'pool_pre_ping': True}
app.config['MAX_CONTENT_LENGTH'] = 64 * 1024 * 1024

TELEGRAM_BOT_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN', '8684244776:AAEjz9Lv8Zc5u-o6BJoHM3eCGXDBQE6hRUU')

db = SQLAlchemy(app)
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login'

# ==============================================================================
# 2. UTILS
# ==============================================================================

def remove_accents(input_str):
    if not isinstance(input_str, str): return str(input_str)
    s1 = u'ÀÁÂÃÈÉÊÌÍÒÓÔÕÙÚÝàáâãèéêìíòóôõùúýĂăĐđĨíŨũƠơƯưẠạẢảẤấẦầẨẩẪẫẬậẮắẰằẲẳẴẵẶặẸẹẺẻẼẽẾếỀềỂểỄễỆệỈỉỊịỌọỎỏỐốỒồỔổỖỗỘộỚớỜờỞởỠỡỢợỤụỦủỨứỪừỬửỮữỰựỲỳỴịỶảỸỹ'
    s0 = u'AAAAEEEIIOOOOUUYaaaaeeeiioooouuyAaDdIiUuOoUuAaAaAaAaAaAaAaAaAaAaAaAaEeEeEeEeEeEeEeEeIiIiOoOoOoOoOoOoOoOoOoOoOoOoOoUuUuUuUuUuUuUuYyYyYaYy'
    s = ''
    for c in input_str:
        if c in s1: s += s0[s1.index(c)]
        else: s += c
    return s

def clean_header(col_name):
    col_name = str(col_name).strip()
    special_map = {
        'ENodeBID': 'enodeb_id', 'gNodeB ID': 'gnodeb_id', 'GNODEB_ID': 'gnodeb_id',
        'CELL_ID': 'cell_id', 'SITE_NAME': 'site_name', 'CELL_NAME': 'cell_name',
        'Frenquency': 'frequency', 'Frequency': 'frequency',
        'PCI': 'pci', 'TAC': 'tac', 'MIMO': 'mimo',
        'UL Traffic Volume (GB)': 'ul_traffic_volume_gb',
        'DL Traffic Volume (GB)': 'dl_traffic_volume_gb',
        'Total Data Traffic Volume (GB)': 'traffic',
        'Cell Uplink Average Throughput': 'cell_uplink_average_throughput',
        'Cell Downlink Average Throughput': 'cell_downlink_average_throughput',
        'A User Downlink Average Throughput': 'user_dl_avg_throughput',
        'Cell avaibility rate': 'cell_avaibility_rate',
        'SgNB Addition Success Rate': 'sgnb_addition_success_rate',
        'SgNB Abnormal Release Rate': 'sgnb_abnormal_release_rate',
        'CQI_5G': 'cqi_5g', 'CQI_4G': 'cqi_4g',
        'POI': 'poi_name', 'Cell_Code': 'cell_code', 'Site_Code': 'site_code',
        'CSHT_code': 'csht_code', 'Hãng_SX': 'hang_sx', 'Antena': 'antena',
        'Swap': 'swap', 'Start_day': 'start_day', 'Ghi_chú': 'ghi_chu',
        'Anten_height': 'anten_height', 'Azimuth': 'azimuth', 'M_T': 'm_t', 'E_T': 'e_t', 'Total_tilt': 'total_tilt',
        'PSC': 'psc', 'DL_UARFCN': 'dl_uarfcn', 'BSC_LAC': 'bsc_lac', 'CI': 'ci',
        'Latitude': 'latitude', 'Longitude': 'longitude', 'Equipment': 'equipment',
        'nrarfcn': 'nrarfcn', 'Lcrid': 'lcrid', 'Đồng_bộ': 'dong_bo',
        'CellID': 'cellid', 'NetworkTech': 'networktech',
        'CELL': 'cell_code', 'SITE': 'site_code', 'MÃ CELL': 'cell_code', 'MÃ TRẠM': 'site_code',
        'UARFCN': 'dl_uarfcn', 'LAC': 'bsc_lac', 'RNC': 'bsc_lac', 'BSC': 'bsc_lac',
        'TÊN CELL': 'cell_name', 'CELLNAME': 'cell_name', 'TÊN TRẠM': 'site_name', 'CELL ID': 'cell_code', 'SITE ID': 'site_code',
        'LAT': 'latitude', 'LONG': 'longitude', 'KINH ĐỘ': 'longitude', 'VĨ ĐỘ': 'latitude',
        'TILT': 'total_tilt', 'ANTEN': 'antena', 'THIẾT BỊ': 'equipment',
        'FREQ': 'frequency', 'TRẠM': 'site_code', 'NODEB': 'site_code', 'NODEB NAME': 'site_name',
        # MAPPING CHUẨN THEO HÌNH 1 (CELL_3G)
        'Tên người quản lý': 'nguoi_quan_ly',
        'SDT người quản lý': 'sdt_nguoi_quan_ly',
        'Ngày hoạt động': 'ngay_hoat_dong',
        'Hoàn cảnh ra đời': 'hoan_canh_ra_doi',
        'Tên quản lý': 'ten_quan_ly',
        'Azimuth': 'azimuth',
        'Antenna gain': 'antenna_gain',
        'Antenna high': 'antenna_high',
        'Mechainical tilt': 'mechanical_tilt',
        'Mechanical tilt': 'mechanical_tilt',
        'Electrical tilt': 'electrical_tilt',
        'Total tilt': 'total_tilt',
        'Địa chỉ': 'dia_chi',
        'Mã CSHT CỦA CELL': 'csht_cell',
        'Mã CSHT CỦA TRẠM': 'csht_site',
        'Longtitude': 'longitude',
        'Longitude': 'longitude',
        'Latitude': 'latitude',
        'Tên đơn vị': 'ten_don_vi',
        'Tên thiết bị': 'thiet_bi',
        'Tên trên hệ thống': 'ten_tren_he_thong',
        'lac': 'lac',
        'ci': 'ci',
        'dl_psc': 'dl_psc',
        'cpich_power': 'cpich_power',
        'Total power': 'total_power',
        'Băng tần': 'bang_tan',
        'Tên loại trạm': 'ten_loai_tram',
        'Loại ăn ten': 'loai_anten',
        'Antenna Tên hãng SX': 'hang_sx_anten',
        'Antenna Dải tần hoạt động': 'anten_dai_tan',
        'Antenna dùng chung': 'anten_dung_chung',
        'Antenna số port': 'anten_so_port',
        # MAPPING CHUẨN THEO HÌNH 2 (CONFIG3G)
        'Mã Trạm': 'ma_tram',
        'Đơn vị quản lý': 'don_vi_quan_ly',
        'Thiết bị': 'thiet_bi',
        'Mã CSHT': 'ma_csht',
        'Loại trạm': 'loai_tram',
        'Site Name': 'site_name',
        'Cell Name (Alias)': 'cell_name_alias',
        'Cell Name': 'cell_name',
        'RAC': 'rac',
        'DL_UARFCN': 'dl_uarfcn',
        'dlPsc': 'dl_psc',
        'DC_support': 'dc_support',
        'cpichPower': 'cpich_power',
        'totalPower': 'total_power',
        'maxPower': 'max_power',
        'OAM IP': 'oam_ip',
        'MechanicalTilt': 'mechanical_tilt',
        'ElectricalTilt': 'electrical_tilt',
        'TotalTilt': 'total_tilt',
        'AntennaType': 'antenna_type',
        'AntennaHigh': 'antenna_high',
        'AntennaGain': 'antenna_gain'
    }
    col_upper = col_name.upper()
    for key, val in special_map.items():
        if key.upper() == col_upper: return val
    clean = re.sub(r'[^a-z0-9]', '_', remove_accents(col_name).lower())
    clean = re.sub(r'_+', '_', clean)
    common_map = {
        'hang_sx': 'hang_sx', 'ghi_chu': 'ghi_chu', 'dong_bo': 'dong_bo',
        'ten_cell': 'ten_cell', 'thoi_gian': 'thoi_gian', 'nha_cung_cap': 'nha_cung_cap',
        'traffic_vol_dl': 'traffic_vol_dl', 'res_blk_dl': 'res_blk_dl',
        'pstraffic': 'pstraffic', 'csconges': 'csconges', 'psconges': 'psconges',
        'cs_so_att': 'cs_so_att', 'ps_so_att': 'ps_so_att',
        'service_drop_all': 'service_drop_all', 'user_dl_avg_thput': 'user_dl_avg_thput',
        'poi': 'poi_name', 'cell_code': 'cell_code', 'site_code': 'site_code'
    }
    return common_map.get(clean, clean)

def generate_colors(n):
    base = ['#0078d4', '#107c10', '#d13438', '#ffaa44', '#00bcf2', '#5c2d91', '#e3008c', '#b4009e']
    if n <= len(base): return base[:n]
    return base + ["#"+''.join([random.choice('0123456789ABCDEF') for j in range(6)]) for i in range(n - len(base))]

# ==============================================================================
# 3. MODELS
# ==============================================================================

class User(UserMixin, db.Model):
    __tablename__ = 'user'
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(50), unique=True, nullable=False)
    password_hash = db.Column(db.String(255), nullable=False)
    role = db.Column(db.String(20), default='user')
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    def set_password(self, password): self.password_hash = generate_password_hash(password)
    def check_password(self, password): return check_password_hash(self.password_hash, password)

class Cell3G(db.Model):
    __tablename__ = 'cell_3g'
    id = db.Column(db.Integer, primary_key=True)
    cell_code = db.Column(db.String(100), index=True)
    site_code = db.Column(db.String(50))
    nguoi_quan_ly = db.Column(db.String(100))
    sdt_nguoi_quan_ly = db.Column(db.String(50))
    ngay_hoat_dong = db.Column(db.String(50))
    hoan_canh_ra_doi = db.Column(db.Text)
    ten_quan_ly = db.Column(db.String(100))
    azimuth = db.Column(db.Integer)
    antenna_gain = db.Column(db.Float)
    antenna_high = db.Column(db.Float)
    mechanical_tilt = db.Column(db.Float)
    electrical_tilt = db.Column(db.Float)
    total_tilt = db.Column(db.Float)
    dia_chi = db.Column(db.String(255))
    csht_cell = db.Column(db.String(50))
    csht_site = db.Column(db.String(50))
    longitude = db.Column(db.Float)
    latitude = db.Column(db.Float)
    ten_don_vi = db.Column(db.String(100))
    thiet_bi = db.Column(db.String(50))
    ten_tren_he_thong = db.Column(db.String(100))
    lac = db.Column(db.String(50))
    ci = db.Column(db.String(50))
    dl_psc = db.Column(db.String(50))
    cpich_power = db.Column(db.Float)
    total_power = db.Column(db.Float)
    bang_tan = db.Column(db.String(50))
    ten_loai_tram = db.Column(db.String(50))
    loai_anten = db.Column(db.String(50))
    hang_sx_anten = db.Column(db.String(50))
    anten_dai_tan = db.Column(db.String(100))
    anten_dung_chung = db.Column(db.String(50))
    anten_so_port = db.Column(db.String(20))
    extra_data = db.Column(db.Text)

class Config3G(db.Model):
    __tablename__ = 'config_3g'
    id = db.Column(db.Integer, primary_key=True)
    ma_tram = db.Column(db.String(50))
    don_vi_quan_ly = db.Column(db.String(100))
    thiet_bi = db.Column(db.String(50))
    ma_csht = db.Column(db.String(50))
    latitude = db.Column(db.Float)
    longitude = db.Column(db.Float)
    loai_tram = db.Column(db.String(50))
    site_name = db.Column(db.String(100))
    cell_name_alias = db.Column(db.String(100))
    cell_name = db.Column(db.String(100), index=True)
    lac = db.Column(db.String(50))
    ci = db.Column(db.String(50))
    rac = db.Column(db.String(50))
    bang_tan = db.Column(db.String(50))
    dl_uarfcn = db.Column(db.String(50))
    dl_psc = db.Column(db.String(50))
    dc_support = db.Column(db.String(50))
    cpich_power = db.Column(db.Float)
    total_power = db.Column(db.Float)
    max_power = db.Column(db.Float)
    oam_ip = db.Column(db.String(50))
    azimuth = db.Column(db.Integer)
    mechanical_tilt = db.Column(db.Float)
    electrical_tilt = db.Column(db.Float)
    total_tilt = db.Column(db.Float)
    antenna_type = db.Column(db.String(100))
    antenna_high = db.Column(db.Float)
    antenna_gain = db.Column(db.Float)
    extra_data = db.Column(db.Text)

class RF3G(db.Model):
    __tablename__ = 'rf_3g'
    id = db.Column(db.Integer, primary_key=True)
    cell_code = db.Column(db.String(100), index=True)
    site_code = db.Column(db.String(50))
    cell_name = db.Column(db.String(100))
    csht_code = db.Column(db.String(50))
    latitude = db.Column(db.Float)
    longitude = db.Column(db.Float)
    antena = db.Column(db.String(100))
    azimuth = db.Column(db.Integer)
    total_tilt = db.Column(db.Float)
    equipment = db.Column(db.String(50))
    frequency = db.Column(db.String(50))
    psc = db.Column(db.String(50))
    dl_uarfcn = db.Column(db.String(50))
    bsc_lac = db.Column(db.String(50))
    ci = db.Column(db.String(50))
    anten_height = db.Column(db.Float)
    m_t = db.Column(db.Float)
    e_t = db.Column(db.Float)
    hang_sx = db.Column(db.String(50))
    swap = db.Column(db.String(50))
    start_day = db.Column(db.String(50))
    ghi_chu = db.Column(db.String(255))
    # Mở rộng trọn bộ các trường từ Cell3G/Config3G
    don_vi_quan_ly = db.Column(db.String(100))
    ma_csht = db.Column(db.String(50))
    antenna_type = db.Column(db.String(100))
    antenna_gain = db.Column(db.Float)
    total_power = db.Column(db.Float)
    oam_ip = db.Column(db.String(50))
    extra_data = db.Column(db.Text)

# --- Other Models (RF4G, RF5G, KPI...) keep as is ---
class RF4G(db.Model):
    __tablename__ = 'rf_4g'
    id = db.Column(db.Integer, primary_key=True)
    cell_code = db.Column(db.String(50), index=True)
    site_code = db.Column(db.String(50))
    cell_name = db.Column(db.String(100))
    csht_code = db.Column(db.String(50))
    latitude = db.Column(db.Float)
    longitude = db.Column(db.Float)
    antena = db.Column(db.String(100))
    azimuth = db.Column(db.Integer)
    total_tilt = db.Column(db.Float)
    equipment = db.Column(db.String(50))
    frequency = db.Column(db.String(50))
    dl_uarfcn = db.Column(db.String(50))
    pci = db.Column(db.String(50))
    tac = db.Column(db.String(50))
    enodeb_id = db.Column(db.String(50))
    lcrid = db.Column(db.String(50))
    anten_height = db.Column(db.Float)
    m_t = db.Column(db.Float)
    e_t = db.Column(db.Float)
    mimo = db.Column(db.String(50))
    hang_sx = db.Column(db.String(50))
    swap = db.Column(db.String(50))
    start_day = db.Column(db.String(50))
    ghi_chu = db.Column(db.String(255))

class RF5G(db.Model):
    __tablename__ = 'rf_5g'
    id = db.Column(db.Integer, primary_key=True)
    cell_code = db.Column(db.String(50), index=True)
    site_code = db.Column(db.String(50))
    site_name = db.Column(db.String(100))
    csht_code = db.Column(db.String(50))
    latitude = db.Column(db.Float)
    longitude = db.Column(db.Float)
    antena = db.Column(db.String(100))
    azimuth = db.Column(db.Integer)
    total_tilt = db.Column(db.Float)
    equipment = db.Column(db.String(50))
    frequency = db.Column(db.String(50))
    nrarfcn = db.Column(db.String(50))
    pci = db.Column(db.String(50))
    tac = db.Column(db.String(50))
    gnodeb_id = db.Column(db.String(50))
    lcrid = db.Column(db.String(50))
    anten_height = db.Column(db.Float)
    m_t = db.Column(db.Float)
    e_t = db.Column(db.Float)
    mimo = db.Column(db.String(50))
    hang_sx = db.Column(db.String(50))
    dong_bo = db.Column(db.String(50))
    start_day = db.Column(db.String(50))
    ghi_chu = db.Column(db.String(255))

class POI4G(db.Model):
    __tablename__ = 'poi_4g'
    id = db.Column(db.Integer, primary_key=True)
    cell_code = db.Column(db.String(50))
    site_code = db.Column(db.String(50))
    poi_name = db.Column(db.String(200), index=True)

class POI5G(db.Model):
    __tablename__ = 'poi_5g'
    id = db.Column(db.Integer, primary_key=True)
    cell_code = db.Column(db.String(50))
    site_code = db.Column(db.String(50))
    poi_name = db.Column(db.String(200), index=True)

class KPI3G(db.Model):
    __tablename__ = 'kpi_3g'
    id = db.Column(db.Integer, primary_key=True)
    ten_cell = db.Column(db.String(100), index=True)
    thoi_gian = db.Column(db.String(50))
    traffic = db.Column(db.Float)
    pstraffic = db.Column(db.Float)
    cssr = db.Column(db.Float)
    dcr = db.Column(db.Float)
    ps_cssr = db.Column(db.Float)
    ps_dcr = db.Column(db.Float)
    hsdpa_throughput = db.Column(db.Float)
    hsupa_throughput = db.Column(db.Float)
    cs_so_att = db.Column(db.Float)
    ps_so_att = db.Column(db.Float)
    csconges = db.Column(db.Float)
    psconges = db.Column(db.Float)
    stt = db.Column(db.String(50))
    nha_cung_cap = db.Column(db.String(50))
    tinh = db.Column(db.String(50))
    ten_rnc = db.Column(db.String(100))
    ma_vnp = db.Column(db.String(50))
    loai_ne = db.Column(db.String(50))
    lac = db.Column(db.String(50))
    ci = db.Column(db.String(50))

class KPI4G(db.Model):
    __tablename__ = 'kpi_4g'
    id = db.Column(db.Integer, primary_key=True)
    ten_cell = db.Column(db.String(100), index=True)
    thoi_gian = db.Column(db.String(50))
    traffic = db.Column(db.Float)
    traffic_vol_dl = db.Column(db.Float)
    traffic_vol_ul = db.Column(db.Float)
    cell_dl_avg_thputs = db.Column(db.Float)
    cell_ul_avg_thput = db.Column(db.Float)
    user_dl_avg_thput = db.Column(db.Float)
    user_ul_avg_thput = db.Column(db.Float)
    erab_ssrate_all = db.Column(db.Float)
    service_drop_all = db.Column(db.Float)
    unvailable = db.Column(db.Float)
    res_blk_dl = db.Column(db.Float)
    cqi_4g = db.Column(db.Float)
    stt = db.Column(db.String(50))
    nha_cung_cap = db.Column(db.String(50))
    tinh = db.Column(db.String(50))
    ten_rnc = db.Column(db.String(100))
    ma_vnp = db.Column(db.String(50))
    loai_ne = db.Column(db.String(50))
    enodeb_id = db.Column(db.String(50))
    cell_id = db.Column(db.String(50))

class KPI5G(db.Model):
    __tablename__ = 'kpi_5g'
    id = db.Column(db.Integer, primary_key=True)
    ten_cell = db.Column(db.String(100), index=True)
    thoi_gian = db.Column(db.String(50))
    traffic = db.Column(db.Float)
    dl_traffic_volume_gb = db.Column(db.Float)
    ul_traffic_volume_gb = db.Column(db.Float)
    cell_downlink_average_throughput = db.Column(db.Float)
    cell_uplink_average_throughput = db.Column(db.Float)
    user_dl_avg_throughput = db.Column(db.Float)
    cqi_5g = db.Column(db.Float)
    cell_avaibility_rate = db.Column(db.Float)
    sgnb_addition_success_rate = db.Column(db.Float)
    sgnb_abnormal_release_rate = db.Column(db.Float)
    nha_cung_cap = db.Column(db.String(50))
    tinh = db.Column(db.String(50))
    ten_gnodeb = db.Column(db.String(100))
    ma_vnp = db.Column(db.String(50))
    loai_ne = db.Column(db.String(50))
    gnodeb_id = db.Column(db.String(50))
    cell_id = db.Column(db.String(50))

class QoE4G(db.Model):
    __tablename__ = 'qoe_4g'
    id = db.Column(db.Integer, primary_key=True)
    cell_name = db.Column(db.String(100), index=True)
    week_name = db.Column(db.String(100))
    qoe_score = db.Column(db.Float)
    qoe_percent = db.Column(db.Float)
    details = db.Column(db.Text)

class QoS4G(db.Model):
    __tablename__ = 'qos_4g'
    id = db.Column(db.Integer, primary_key=True)
    cell_name = db.Column(db.String(100), index=True)
    week_name = db.Column(db.String(100))
    qos_score = db.Column(db.Float)
    qos_percent = db.Column(db.Float)
    details = db.Column(db.Text)

class ITSLog(db.Model):
    __tablename__ = 'its_log'
    id = db.Column(db.Integer, primary_key=True)
    timestamp = db.Column(db.String(50))
    latitude = db.Column(db.Float)
    longitude = db.Column(db.Float)
    networktech = db.Column(db.String(20))
    level = db.Column(db.Float)
    qual = db.Column(db.Float)
    cellid = db.Column(db.String(50))

@login_manager.user_loader
def load_user(user_id):
    return db.session.get(User, int(user_id))

def init_database():
    with app.app_context():
        db.create_all()
        if not User.query.filter_by(username='admin').first():
            u = User(username='admin', role='admin')
            u.set_password('admin123')
            db.session.add(u)
            db.session.commit()
init_database()

# ==============================================================================
# 4. TEMPLATES (DEFINED AT MODULE LEVEL - DO NOT INDENT)
# ==============================================================================

BASE_LAYOUT = """
<!DOCTYPE html>
<html lang="vi">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>KPI Monitor System</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css">
    <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" crossorigin=""/>
    <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js" crossorigin=""></script>
    <link href='https://cdnjs.cloudflare.com/ajax/libs/leaflet.fullscreen/3.0.0/Control.FullScreen.min.css' rel='stylesheet' />
    <script src='https://cdnjs.cloudflare.com/ajax/libs/leaflet.fullscreen/3.0.0/Control.FullScreen.min.js'></script>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        :root { --acrylic-bg: rgba(255, 255, 255, 0.6); --acrylic-blur: blur(20px); --sidebar-bg: rgba(240, 240, 245, 0.85); --primary-color: #0078d4; --text-color: #212529; --shadow-soft: 0 4px 12px rgba(0, 0, 0, 0.05); --shadow-hover: 0 8px 16px rgba(0, 0, 0, 0.1); --border-radius: 12px; }
        body { background: linear-gradient(135deg, #f3f4f6 0%, #eef2f3 100%); font-family: 'Segoe UI', sans-serif; color: var(--text-color); overflow-x: hidden; }
        .sidebar { height: 100vh; width: 260px; position: fixed; top: 0; left: 0; background: var(--sidebar-bg); backdrop-filter: var(--acrylic-blur); border-right: 1px solid rgba(255,255,255,0.5); z-index: 1000; transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1); padding-top: 1rem; overflow-y: auto; padding-bottom: 60px; }
        .sidebar-header { padding: 1.5rem; color: var(--primary-color); font-weight: 600; font-size: 1.5rem; text-align: center; letter-spacing: 0.5px; }
        .sidebar-menu { padding: 0; list-style: none; margin: 1rem 0; }
        .sidebar-menu a { display: flex; align-items: center; padding: 14px 25px; color: #555; text-decoration: none; font-weight: 500; border-left: 4px solid transparent; transition: all 0.2s ease; margin: 4px 12px; border-radius: 8px; }
        .sidebar-menu a:hover, .sidebar-menu a.active { background-color: rgba(255, 255, 255, 0.8); color: var(--primary-color); box-shadow: 0 2px 8px rgba(0,0,0,0.05); }
        .sidebar-menu a.active { border-left-color: var(--primary-color); background-color: rgba(255, 255, 255, 0.95); }
        .sidebar-menu i { margin-right: 15px; width: 24px; text-align: center; font-size: 1.1rem; }
        .main-content { margin-left: 260px; padding: 30px; min-height: 100vh; transition: all 0.3s ease; }
        .card { border: none; border-radius: var(--border-radius); background: rgba(255, 255, 255, 0.85); backdrop-filter: blur(10px); box-shadow: var(--shadow-soft); transition: box-shadow 0.3s ease; margin-bottom: 1.5rem; }
        .card:hover { box-shadow: var(--shadow-hover); }
        .card-header { background-color: rgba(255, 255, 255, 0.9); border-bottom: 1px solid rgba(0,0,0,0.05); padding: 1.25rem 1.5rem; font-weight: 600; color: #333; font-size: 1.1rem; }
        .card-body { padding: 1.5rem; }
        .btn-primary { background-color: var(--primary-color); border: none; box-shadow: 0 2px 6px rgba(0, 120, 212, 0.3); border-radius: 6px; padding: 0.5rem 1.25rem; font-weight: 500; transition: all 0.2s; }
        .btn-primary:hover { background-color: #0063b1; box-shadow: 0 4px 12px rgba(0, 120, 212, 0.4); transform: translateY(-1px); }
        .btn-warning { background-color: #ffaa44; border: none; box-shadow: 0 2px 6px rgba(255, 170, 68, 0.3); border-radius: 6px; padding: 0.5rem 1.25rem; font-weight: 500; color: #fff; transition: all 0.2s; }
        .btn-warning:hover { background-color: #e69532; color: #fff; box-shadow: 0 4px 12px rgba(255, 170, 68, 0.4); transform: translateY(-1px); }
        .table { background: transparent; }
        .table thead th { background-color: rgba(248, 249, 250, 0.8); border-bottom: 2px solid #e9ecef; color: #555; font-weight: 600; font-size: 0.9rem; text-transform: uppercase; }
        .table-hover tbody tr:hover { background-color: rgba(0, 120, 212, 0.05); }
        .chart-container canvas { cursor: zoom-in; }
        @media (max-width: 768px) { .sidebar { margin-left: -260px; } .sidebar.active { margin-left: 0; } .main-content { margin-left: 0; padding: 15px; } }
        .legend { line-height: 18px; color: #333; }
        .legend i { width: 14px; height: 14px; float: left; margin-right: 8px; opacity: 0.8; border: 1px solid #999; }
        .table-responsive::-webkit-scrollbar { height: 8px; width: 8px; }
        .table-responsive::-webkit-scrollbar-track { background: #f1f1f1; border-radius: 4px; }
        .table-responsive::-webkit-scrollbar-thumb { background: #c1c1c1; border-radius: 4px; }
        .table-responsive::-webkit-scrollbar-thumb:hover { background: #a8a8a8; }
        /* CSS Overlay cho Sidebar trên Mobile */
        #sidebar-overlay { display: none; position: fixed; top: 0; left: 0; width: 100vw; height: 100vh; background: rgba(0,0,0,0.4); z-index: 999; backdrop-filter: blur(2px); transition: all 0.3s ease; }
        #sidebar-overlay.active { display: block; }
    </style>
</head>
<body>
    <div id="sidebar-overlay" onclick="toggleSidebar()"></div>
    <div class="sidebar" id="sidebar">
        <div class="sidebar-header"><i class="fa-solid fa-network-wired"></i> NetOps</div>
        <ul class="sidebar-menu">
            <li><a href="/" class="{{ 'active' if active_page == 'dashboard' else '' }}"><i class="fa-solid fa-gauge"></i> Dashboard</a></li>
            <li><a href="/gis" class="{{ 'active' if active_page == 'gis' else '' }}"><i class="fa-solid fa-map-location-dot"></i> Bản đồ GIS</a></li>
            <li><a href="/kpi" class="{{ 'active' if active_page == 'kpi' else '' }}"><i class="fa-solid fa-chart-line"></i> KPI Analytics</a></li>
            <li><a href="/qoe-qos" class="{{ 'active' if active_page == 'qoe_qos' else '' }}"><i class="fa-solid fa-star-half-stroke"></i> QoE QoS Analytics</a></li>
            <li><a href="/optimize" class="{{ 'active' if active_page == 'optimize' else '' }}"><i class="fa-solid fa-wand-magic-sparkles"></i> Tối ưu QoE/QoS</a></li>
            <li><a href="/rf" class="{{ 'active' if active_page == 'rf' else '' }}"><i class="fa-solid fa-tower-broadcast"></i> RF Database</a></li>
            <li><a href="/poi" class="{{ 'active' if active_page == 'poi' else '' }}"><i class="fa-solid fa-map-pin"></i> POI Report</a></li>
            <li><a href="/worst-cell" class="{{ 'active' if active_page == 'worst_cell' else '' }}"><i class="fa-solid fa-triangle-exclamation"></i> Worst Cells</a></li>
            <li><a href="/conges-3g" class="{{ 'active' if active_page == 'conges_3g' else '' }}"><i class="fa-solid fa-users-slash"></i> Congestion 3G</a></li>
            <li><a href="/traffic-down" class="{{ 'active' if active_page == 'traffic_down' else '' }}"><i class="fa-solid fa-arrow-trend-down"></i> Traffic Down</a></li>
            <li>
                <a href="#toolsMenu" data-bs-toggle="collapse" class="{{ 'active' if active_page in ['azimuth', 'script'] else '' }}">
                    <i class="fa-solid fa-toolbox"></i> Tools
                    <i class="fa-solid fa-chevron-down ms-auto" style="width: auto; font-size: 0.8rem;"></i>
                </a>
                <ul class="collapse list-unstyled {{ 'show' if active_page in ['azimuth', 'script'] else '' }}" id="toolsMenu">
                    <li><a href="/azimuth" class="{{ 'active' if active_page == 'azimuth' else '' }}" style="margin-left: 20px; font-size: 0.95rem;"><i class="fa-solid fa-compass"></i> Azimuth</a></li>
                    <li><a href="/script" class="{{ 'active' if active_page == 'script' else '' }}" style="margin-left: 20px; font-size: 0.95rem;"><i class="fa-solid fa-code"></i> Script</a></li>
                </ul>
            </li>
            {% if current_user.role == 'admin' %}
            <li><a href="/import" class="{{ 'active' if active_page == 'import' else '' }}"><i class="fa-solid fa-cloud-arrow-up"></i> Data Import</a></li>
            <li class="mt-4 mb-2 text-muted px-4 text-uppercase" style="font-size: 0.75rem; letter-spacing: 1px;">System</li>
            <li><a href="/users" class="{{ 'active' if active_page == 'users' else '' }}"><i class="fa-solid fa-users-gear"></i> User Mgmt</a></li>
            <li><a href="/backup-restore" class="{{ 'active' if active_page == 'backup_restore' else '' }}"><i class="fa-solid fa-database"></i> Backup / Restore</a></li>
            {% endif %}
            <li><a href="/profile" class="{{ 'active' if active_page == 'profile' else '' }}"><i class="fa-solid fa-user-shield"></i> Profile</a></li>
            <li><a href="/logout"><i class="fa-solid fa-right-from-bracket"></i> Logout</a></li>
        </ul>
    </div>

    <div class="main-content">
        <button class="btn btn-light shadow-sm d-md-none mb-3 border fw-bold" onclick="toggleSidebar()">
            <i class="fa-solid fa-bars me-1"></i> Menu
        </button>

        <div class="container-fluid p-0">
            {% with messages = get_flashed_messages(with_categories=true) %}
                {% if messages %}
                    {% for category, message in messages %}
                        <div class="alert alert-{{ category }} alert-dismissible fade show shadow-sm border-0 mb-4" role="alert">
                            {{ message }}
                            <button type="button" class="btn-close" data-bs-dismiss="alert" aria-label="Close"></button>
                        </div>
                    {% endfor %}
                {% endif %}
            {% endwith %}

            <!-- Dynamic Content -->
            {% block content %}{% endblock %}
        </div>
    </div>
    
    <div class="modal fade" id="chartDetailModal" tabindex="-1" aria-hidden="true">
        <div class="modal-dialog modal-xl modal-dialog-centered">
            <div class="modal-content border-0 shadow-lg" style="background: rgba(255,255,255,0.95); backdrop-filter: blur(15px);">
                <div class="modal-header border-0 pb-0">
                    <h5 class="modal-title text-primary fw-bold" id="modalTitle"></h5>
                    <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
                </div>
                <div class="modal-body p-4">
                    <div class="chart-container" style="position: relative; height:65vh; width:100%">
                        <canvas id="modalChart"></canvas>
                    </div>
                </div>
            </div>
        </div>
    </div>

    <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/js/bootstrap.bundle.min.js"></script>
    <script>
        function toggleSidebar() {
            document.getElementById('sidebar').classList.toggle('active');
            document.getElementById('sidebar-overlay').classList.toggle('active');
        }

        let modalChartInstance = null;
        function showDetailModal(cellName, date, value, metricLabel, allDatasets, allLabels) {
            document.getElementById('modalTitle').innerText = 'Chi tiết ' + metricLabel;
            const ctx = document.getElementById('modalChart').getContext('2d');
            if (modalChartInstance) modalChartInstance.destroy();
            modalChartInstance = new Chart(ctx, {
                type: 'line',
                data: { labels: allLabels, datasets: allDatasets },
                options: { responsive: true, maintainAspectRatio: false, interaction: { mode: 'nearest', intersect: false }, plugins: { legend: { display: true } }, spanGaps: true, elements: { line: { tension: 0.3 } } }
            });
            new bootstrap.Modal(document.getElementById('chartDetailModal')).show();
        }
        
        function toggleCheckboxes(source) {
            let checkboxes = document.getElementsByName('tables');
            for(var i=0; i<checkboxes.length; i++) checkboxes[i].checked = source.checked;
        }
        
        function addRow(tech) {
            var table = document.getElementById("rruTable_" + tech).getElementsByTagName('tbody')[0];
            var newRow = table.insertRow(table.rows.length);
            var idx = table.rows.length;
            var defaultSRN, defaultSlot;
            if (tech == '3g900') { defaultSRN = 70 + idx - 1; defaultSlot = 2; }
            else if (tech == '3g2100') { defaultSRN = 80 + idx - 1; defaultSlot = 3; }
            else { defaultSRN = 60 + idx - 1; defaultSlot = 3; }

            newRow.insertCell(0).innerHTML = `<input type="text" name="rn[]" class="form-control" value="RRU${idx}">`;
            newRow.insertCell(1).innerHTML = `<input type="number" name="srn[]" class="form-control" value="${defaultSRN}">`;
            newRow.insertCell(2).innerHTML = `<input type="number" name="hsn[]" class="form-control" value="${defaultSlot}">`;
            newRow.insertCell(3).innerHTML = `<input type="number" name="hpn[]" class="form-control" value="${idx-1}">`;
            newRow.insertCell(4).innerHTML = `<input type="number" name="rcn[]" class="form-control" value="${idx-1}">`;
            newRow.insertCell(5).innerHTML = `<input type="number" name="sectorid[]" class="form-control" value="${idx-1}">`;
            newRow.insertCell(6).innerHTML = `<input type="number" name="rxnum[]" class="form-control" value="${tech=='4g'?4:2}">`;
            newRow.insertCell(7).innerHTML = `<input type="number" name="txnum[]" class="form-control" value="${tech=='4g'?4:1}">`;
            newRow.insertCell(8).innerHTML = `<button type="button" class="btn btn-danger btn-sm" onclick="this.closest('tr').remove()">X</button>`;
        }
    </script>
</body>
</html>
"""

LOGIN_PAGE = """
<!DOCTYPE html>
<html lang="vi">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Đăng nhập | NetOps</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
    <style>
        body { background: linear-gradient(135deg, #f0f2f5 0%, #d9e2ec 100%); height: 100vh; display: flex; align-items: center; justify-content: center; font-family: 'Segoe UI', sans-serif; }
        .login-card { width: 100%; max-width: 400px; background: rgba(255, 255, 255, 0.9); backdrop-filter: blur(20px); padding: 40px; border-radius: 16px; box-shadow: 0 10px 30px rgba(0,0,0,0.08); border: 1px solid rgba(255,255,255,0.5); }
        .btn-primary { background-color: #0078d4; border: none; padding: 10px; font-weight: 600; border-radius: 8px; transition: all 0.3s; width: 100%; }
        .btn-primary:hover { background-color: #0063b1; transform: translateY(-2px); box-shadow: 0 4px 12px rgba(0, 120, 212, 0.3); }
        .form-control { border-radius: 8px; padding: 12px; margin-bottom: 1rem; border: 1px solid #e0e0e0; }
        .form-control:focus { box-shadow: 0 0 0 3px rgba(0, 120, 212, 0.15); border-color: #0078d4; }
    </style>
</head>
<body>
    <div class="login-card">
        <h3 class="text-center mb-4 text-primary fw-bold">Welcome Back</h3>
        <p class="text-center text-muted mb-4">Sign in to access KPI Monitor</p>
        <form method="POST">
            <label class="form-label fw-bold text-secondary small">USERNAME</label>
            <input type="text" name="username" class="form-control" placeholder="Enter username" required>
            <label class="form-label fw-bold text-secondary small">PASSWORD</label>
            <input type="password" name="password" class="form-control" placeholder="Enter password" required>
            <button type="submit" class="btn btn-primary">Sign In</button>
        </form>
    </div>
</body>
</html>
"""

CONTENT_TEMPLATE = """
{% extends "base" %}
{% block content %}
<div class="card border-0 shadow-sm mb-4">
    <div class="card-header d-flex justify-content-between align-items-center bg-white border-bottom">
        <span class="fs-5 fw-bold text-secondary">{{ title }}</span>
        <span class="badge bg-soft-primary text-primary px-3 py-2 rounded-pill border">{{ current_user.role | upper }}</span>
    </div>
    <div class="card-body">
        {% if active_page == 'dashboard' %}
            <h5 class="fw-bold text-secondary mb-4"><i class="fa-solid fa-chart-pie me-2"></i>Tổng quan Chất lượng Mạng 4G Toàn Hệ thống</h5>
            {% if dashboard_data and dashboard_data.labels %}
                <div class="row g-4">
                    <div class="col-md-6"><div class="card border border-light shadow-sm h-100"><div class="card-body p-3"><h6 class="fw-bold text-primary mb-3">Tổng Traffic 4G (GB)</h6><div class="chart-container" style="position: relative; height:30vh; width:100%"><canvas id="chartTraffic"></canvas></div></div></div></div>
                    <div class="col-md-6"><div class="card border border-light shadow-sm h-100"><div class="card-body p-3"><h6 class="fw-bold text-success mb-3">Trung bình User DL Thput (Mbps)</h6><div class="chart-container" style="position: relative; height:30vh; width:100%"><canvas id="chartThput"></canvas></div></div></div></div>
                    <div class="col-md-6"><div class="card border border-light shadow-sm h-100"><div class="card-body p-3"><h6 class="fw-bold text-warning mb-3">Trung bình Tài nguyên PRB DL (%)</h6><div class="chart-container" style="position: relative; height:30vh; width:100%"><canvas id="chartPrb"></canvas></div></div></div></div>
                    <div class="col-md-6"><div class="card border border-light shadow-sm h-100"><div class="card-body p-3"><h6 class="fw-bold text-info mb-3">Trung bình Chất lượng Vô tuyến (CQI 4G)</h6><div class="chart-container" style="position: relative; height:30vh; width:100%"><canvas id="chartCqi"></canvas></div></div></div></div>
                </div>
                <script>
                    document.addEventListener('DOMContentLoaded', function() {
                        const labels = {{ dashboard_data.labels | tojson }};
                        function createDashChart(id, label, color, bgColor, dataArr, titleStr) {
                            const ds = [{ label: label, data: dataArr, borderColor: color, backgroundColor: bgColor, fill: true, tension: 0.3, borderWidth: 2 }];
                            new Chart(document.getElementById(id).getContext('2d'), { type: 'line', data: { labels: labels, datasets: ds }, options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { display: false } }, interaction: { mode: 'nearest', axis: 'x', intersect: false }, onClick: (e, el) => { if (el.length > 0) showDetailModal(ds[0].label, labels[el[0].index], ds[0].data[el[0].index], titleStr, ds, labels); } } });
                        }
                        createDashChart('chartTraffic', 'Traffic (GB)', '#0078d4', 'rgba(0,120,212,0.1)', {{ dashboard_data.traffic | tojson }}, 'Tổng Traffic 4G');
                        createDashChart('chartThput', 'Avg Thput (Mbps)', '#107c10', 'rgba(16,124,16,0.1)', {{ dashboard_data.thput | tojson }}, 'Trung bình Thput');
                        createDashChart('chartPrb', 'Avg PRB (%)', '#ffaa44', 'rgba(255,170,68,0.1)', {{ dashboard_data.prb | tojson }}, 'Trung bình PRB');
                        createDashChart('chartCqi', 'Avg CQI', '#00bcf2', 'rgba(0,188,242,0.1)', {{ dashboard_data.cqi | tojson }}, 'Trung bình CQI');
                    });
                </script>
            {% else %}
                <div class="alert alert-info border-0 shadow-sm"><i class="fa-solid fa-circle-info me-2"></i>Chưa có dữ liệu KPI 4G để hiển thị biểu đồ.</div>
            {% endif %}

        {% elif active_page == 'azimuth' %}
            <div class="card border border-light shadow-sm">
                <div class="card-body p-1">
                    <div id="azimuthMap" style="height: 75vh; width: 100%; border-radius: 8px; z-index: 1;"></div>
                </div>
            </div>

            <!-- Panel công cụ (Sẽ được gắn vào trong Bản đồ như một Control Nổi) -->
            <div id="azimuthFormContainer" class="shadow-lg" style="background: rgba(255, 255, 255, 0.95); padding: 15px; border-radius: 8px; width: 320px; max-width: 90vw; max-height: 65vh; overflow-y: auto; border: 1px solid #dee2e6;">
                <h6 class="fw-bold text-primary mb-2"><i class="fa-solid fa-compass me-2"></i>Tọa độ Điểm O (Gốc)</h6>
                <div class="mb-2 text-muted" style="font-size: 0.75rem;"><i class="fa-solid fa-info-circle me-1"></i><i>Mẹo: Click lên Bản đồ để chọn nhanh Điểm O</i></div>
                <div class="mb-2">
                    <label class="form-label small fw-bold mb-1">Vĩ độ (Latitude)</label>
                    <input type="text" id="latO" class="form-control form-control-sm" placeholder="VD: 21.028511" required>
                </div>
                <div class="mb-3">
                    <label class="form-label small fw-bold mb-1">Kinh độ (Longitude)</label>
                    <input type="text" id="lngO" class="form-control form-control-sm" placeholder="VD: 105.804817" required>
                </div>
                <button type="button" class="btn btn-outline-secondary btn-sm w-100 mb-3 fw-bold" onclick="getGPS()"><i class="fa-solid fa-location-crosshairs me-1"></i>Lấy GPS của tôi</button>

                <hr class="my-3">

                <h6 class="fw-bold text-success mb-2"><i class="fa-solid fa-pencil me-2"></i>Thêm Điểm Kết Nối</h6>
                <form id="azimuthForm">
                    <div class="mb-2">
                        <label class="form-label small fw-bold mb-1">Tên điểm tới</label>
                        <input type="text" id="ptName" class="form-control form-control-sm" placeholder="VD: Trạm A" required>
                    </div>
                    <div class="mb-2">
                        <label class="form-label small fw-bold mb-1">Góc Azimuth (Độ)</label>
                        <input type="number" id="ptAzimuth" class="form-control form-control-sm" min="0" max="360" step="any" placeholder="0 - 360" required>
                    </div>
                    <div class="mb-3">
                        <label class="form-label small fw-bold mb-1">Khoảng cách (Mét)</label>
                        <input type="number" id="ptDistance" class="form-control form-control-sm" min="0" step="any" placeholder="Nhập số mét..." required>
                    </div>
                    <button type="submit" class="btn btn-primary btn-sm w-100 shadow-sm fw-bold mb-2"><i class="fa-solid fa-plus me-1"></i>Vẽ đường nối</button>
                    <button type="button" class="btn btn-danger btn-sm w-100 shadow-sm fw-bold" onclick="clearDrawnPoints()"><i class="fa-solid fa-trash-can me-1"></i>Xóa các đường đã vẽ</button>
                </form>
            </div>

            <script>
                var azMap, markerO;
                var drawnItems = L.layerGroup();
                var drawnPointsData = []; 

                document.addEventListener('DOMContentLoaded', function() {
                    azMap = L.map('azimuthMap', {
                        center: [16.0, 106.0], 
                        zoom: 5,
                        zoomControl: true, // Mở lại zoom mặc định ở góc topleft
                        fullscreenControl: true, // Bật plugin FullScreen gốc
                        fullscreenControlOptions: { position: 'topleft' } // Đặt ở góc trên bên trái
                    });

                    var googleStreets = L.tileLayer('https://{s}.google.com/vt/lyrs=m&x={x}&y={y}&z={z}', {
                        maxZoom: 22,
                        subdomains: ['mt0', 'mt1', 'mt2', 'mt3'],
                        detectRetina: true,
                        attribution: '© Google Maps'
                    }).addTo(azMap);
                    
                    var googleHybrid = L.tileLayer('https://{s}.google.com/vt/lyrs=y&x={x}&y={y}&z={z}', {
                        maxZoom: 22,
                        subdomains: ['mt0', 'mt1', 'mt2', 'mt3'],
                        detectRetina: true,
                        attribution: '© Google Maps'
                    });

                    L.control.layers({"Bản đồ (Google)": googleStreets, "Vệ tinh (Google)": googleHybrid}, null, {position: 'topright'}).addTo(azMap);
                    
                    drawnItems.addTo(azMap);

                    // --- CHUYỂN FORM VÀO TRONG BẢN ĐỒ VÀ TẠO NÚT TOGGLE ẨN/HIỆN LÊN ĐIỆN THOẠI ---
                    var formControl = L.control({position: 'topright'});
                    formControl.onAdd = function (map) {
                        var wrapper = L.DomUtil.create('div', 'leaflet-control');
                        
                        // Nút thu gọn
                        var toggleBtn = L.DomUtil.create('button', 'btn btn-primary btn-sm shadow-lg mb-2 w-100 fw-bold', wrapper);
                        toggleBtn.innerHTML = '<i class="fa-solid fa-sliders me-1"></i>Công Cụ Vẽ';
                        toggleBtn.style.border = '2px solid white';
                        toggleBtn.style.borderRadius = '8px';

                        var formDiv = document.getElementById('azimuthFormContainer');
                        wrapper.appendChild(formDiv);
                        
                        // Tự động thu gọn form nếu dùng trên điện thoại
                        if (window.innerWidth <= 768) {
                            formDiv.style.display = 'none';
                        }

                        L.DomEvent.disableClickPropagation(wrapper);
                        L.DomEvent.disableScrollPropagation(wrapper);

                        L.DomEvent.on(toggleBtn, 'click', function(e) {
                            L.DomEvent.preventDefault(e);
                            L.DomEvent.stopPropagation(e);
                            if (formDiv.style.display === 'none') {
                                formDiv.style.display = 'block';
                            } else {
                                formDiv.style.display = 'none';
                            }
                        });

                        return wrapper;
                    };
                    formControl.addTo(azMap);

                    // Sự kiện Click lên bản đồ để lấy toạ độ Điểm O
                    azMap.on('click', function(e) {
                        if (!markerO) { // CHỈ TẠO LẦN ĐẦU TIÊN
                            document.getElementById('latO').value = e.latlng.lat.toFixed(6);
                            document.getElementById('lngO').value = e.latlng.lng.toFixed(6);
                            drawOrigin();
                        }
                    });

                    document.getElementById('azimuthForm').addEventListener('submit', function(e) {
                        e.preventDefault();
                        drawAzimuth();
                    });
                });

                function flyToOrigin(lat, lng) {
                    azMap.flyTo([lat, lng], 17, { animate: true, duration: 1.5 });
                }

                function getGPS() {
                    if (navigator.geolocation) {
                        navigator.geolocation.getCurrentPosition(function(position) {
                            document.getElementById('latO').value = position.coords.latitude.toFixed(6);
                            document.getElementById('lngO').value = position.coords.longitude.toFixed(6);
                            flyToOrigin(position.coords.latitude, position.coords.longitude);
                            drawOrigin();
                        }, function(error) {
                            alert("Lỗi không lấy được GPS: " + error.message);
                        });
                    } else {
                        alert("Trình duyệt của bạn không hỗ trợ Geolocation.");
                    }
                }

                function calculateDistanceAndBearing(lat1, lon1, lat2, lon2) {
                    const R = 6371e3;
                    const f1 = lat1 * Math.PI/180;
                    const f2 = lat2 * Math.PI/180;
                    const df = (lat2-lat1) * Math.PI/180;
                    const dl = (lon2-lon1) * Math.PI/180;

                    const a = Math.sin(df/2) * Math.sin(df/2) + Math.cos(f1) * Math.cos(f2) * Math.sin(dl/2) * Math.sin(dl/2);
                    const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
                    const dist = R * c;

                    const y = Math.sin(dl) * Math.cos(f2);
                    const x = Math.cos(f1)*Math.sin(f2) - Math.sin(f1)*Math.cos(f2)*Math.cos(dl);
                    let brng = Math.atan2(y, x) * 180 / Math.PI;
                    brng = (brng + 360) % 360;

                    return { distance: dist, bearing: brng };
                }

                function updateAllLines() {
                    var latO = parseFloat(document.getElementById('latO').value);
                    var lngO = parseFloat(document.getElementById('lngO').value);
                    if(isNaN(latO) || isNaN(lngO)) return;

                    drawnPointsData.forEach(function(obj) {
                        var posB = obj.marker.getLatLng();
                        var calc = calculateDistanceAndBearing(latO, lngO, posB.lat, posB.lng);
                        
                        obj.line.setLatLngs([[latO, lngO], posB]);
                        var popupContent = "<div class='text-center'><b>" + obj.name + "</b><hr class='my-1'>Góc Azimuth: <b class='text-danger'>" + calc.bearing.toFixed(2) + "°</b><br>Khoảng cách: <b class='text-primary'>" + calc.distance.toFixed(2) + " m</b></div>";
                        if(obj.marker.getPopup()) obj.marker.getPopup().setContent(popupContent);
                    });
                }

                function drawOrigin() {
                    var latO = document.getElementById('latO').value;
                    var lngO = document.getElementById('lngO').value;
                    if (!latO || !lngO) return;
                    
                    if (markerO) { azMap.removeLayer(markerO); }
                    
                    var iconO = L.divIcon({className: 'custom-div-icon', html: "<div style='background-color:#c0392b;width:18px;height:18px;border-radius:50%;border:3px solid white;box-shadow:0 0 8px rgba(0,0,0,0.8);'></div>", iconSize: [18, 18], iconAnchor: [9, 9]});
                    
                    markerO = L.marker([latO, lngO], {icon: iconO, draggable: true})
                        .bindTooltip("<b class='text-danger'>Điểm O</b>", {permanent: true, direction: 'left', className: 'bg-white border-danger rounded shadow-sm px-1 py-0'})
                        .addTo(azMap);
                        
                    markerO.on('drag', function(e) {
                        var newPos = e.target.getLatLng();
                        document.getElementById('latO').value = newPos.lat.toFixed(6);
                        document.getElementById('lngO').value = newPos.lng.toFixed(6);
                        updateAllLines();
                    });

                    updateAllLines();
                }

                document.getElementById('latO').addEventListener('change', function() {
                    drawOrigin();
                    var lat = document.getElementById('latO').value;
                    var lng = document.getElementById('lngO').value;
                    if(lat && lng) flyToOrigin(lat, lng);
                });
                
                document.getElementById('lngO').addEventListener('change', function() {
                    drawOrigin();
                    var lat = document.getElementById('latO').value;
                    var lng = document.getElementById('lngO').value;
                    if(lat && lng) flyToOrigin(lat, lng);
                });

                function calculateDestinationPoint(lat1, lon1, brng, dist) {
                    const R = 6371e3;
                    const d = parseFloat(dist);
                    const brngRad = parseFloat(brng) * Math.PI / 180;
                    const lat1Rad = parseFloat(lat1) * Math.PI / 180;
                    const lon1Rad = parseFloat(lon1) * Math.PI / 180;

                    const lat2Rad = Math.asin(Math.sin(lat1Rad) * Math.cos(d/R) + Math.cos(lat1Rad) * Math.sin(d/R) * Math.cos(brngRad));
                    const lon2Rad = lon1Rad + Math.atan2(Math.sin(brngRad) * Math.sin(d/R) * Math.cos(lat1Rad), Math.cos(d/R) - Math.sin(lat1Rad) * Math.sin(lat2Rad));

                    return [lat2Rad * 180 / Math.PI, lon2Rad * 180 / Math.PI];
                }

                function drawAzimuth() {
                    var latO = document.getElementById('latO').value;
                    var lngO = document.getElementById('lngO').value;
                    var ptName = document.getElementById('ptName').value;
                    var az = document.getElementById('ptAzimuth').value;
                    var dist = document.getElementById('ptDistance').value;

                    if (!latO || !lngO || !ptName || !az || !dist) {
                        alert("Vui lòng nhập đủ Điểm O, Tên điểm, Góc và Khoảng cách!");
                        return;
                    }

                    drawOrigin();
                    var pointB = calculateDestinationPoint(latO, lngO, az, dist);

                    var iconB = L.divIcon({className: 'custom-div-icon', html: "<div style='background-color:#2980b9;width:14px;height:14px;border-radius:50%;border:2px solid white;box-shadow:0 0 6px rgba(0,0,0,0.6);'></div>", iconSize: [14, 14], iconAnchor: [7, 7]});
                    
                    var popupContent = "<div class='text-center'><b>" + ptName + "</b><hr class='my-1'>Góc Azimuth: <b class='text-danger'>" + parseFloat(az).toFixed(2) + "°</b><br>Khoảng cách: <b class='text-primary'>" + parseFloat(dist).toFixed(2) + " m</b></div>";
                    
                    var markerB = L.marker(pointB, {icon: iconB, draggable: true})
                        .bindTooltip("<b>" + ptName + "</b>", {permanent: true, direction: 'right', className: 'text-primary border-primary rounded shadow-sm px-1 py-0'})
                        .bindPopup(popupContent, {autoPan: false})
                        .addTo(drawnItems);

                    markerB.on('dragstart', function(e) {
                        this.openPopup();
                    });

                    var polyline = L.polyline([[latO, lngO], pointB], {
                        color: '#000000',
                        weight: 4,
                        opacity: 1.0
                    }).addTo(drawnItems);
                    
                    var drawnObj = { marker: markerB, line: polyline, name: ptName };
                    drawnPointsData.push(drawnObj);
                    
                    markerB.on('drag', function(e) {
                        var newPos = e.target.getLatLng();
                        var curLatO = parseFloat(document.getElementById('latO').value);
                        var curLngO = parseFloat(document.getElementById('lngO').value);
                        
                        var calc = calculateDistanceAndBearing(curLatO, curLngO, newPos.lat, newPos.lng);
                        drawnObj.line.setLatLngs([[curLatO, curLngO], newPos]);
                        
                        var newPopup = "<div class='text-center'><b>" + ptName + "</b><hr class='my-1'>Góc Azimuth: <b class='text-danger'>" + calc.bearing.toFixed(2) + "°</b><br>Khoảng cách: <b class='text-primary'>" + calc.distance.toFixed(2) + " m</b></div>";
                        drawnObj.marker.getPopup().setContent(newPopup);
                    });

                    var group = new L.featureGroup([markerO, drawnItems]);
                    azMap.fitBounds(group.getBounds(), {padding: [50, 50]});
                    
                    document.getElementById('ptName').value = '';
                    document.getElementById('ptAzimuth').value = '';
                    document.getElementById('ptDistance').value = '';
                    document.getElementById('ptName').focus();
                }
                
                function clearDrawnPoints() {
                    drawnItems.clearLayers();
                    drawnPointsData = [];
                    if(markerO) azMap.setView(markerO.getLatLng(), 15);
                }
            </script>
            
        {% elif active_page == 'optimize' %}
            <div class="alert alert-info border-0 shadow-sm mb-4">
                <h5 class="fw-bold text-primary mb-3"><i class="fa-solid fa-book-open-reader me-2"></i>Quy trình Tối ưu 5 Bước</h5>
                <ol class="mb-0 text-dark">
                    <li class="mb-1"><strong>Thu thập Vĩ mô:</strong> Tự động lọc Top Cell tệ từ báo cáo QoE/QoS Tuần.</li>
                    <li class="mb-1"><strong>Chẩn đoán Vi mô:</strong> Tự động ghép nối KPI Ngày để tìm nguyên nhân gốc rễ (Nghẽn, Nhiễu, Lỗi Phần cứng).</li>
                    <li class="mb-1"><strong>Giải pháp Tối ưu:</strong> Đề xuất hành động cho RNO, NOC, UCTT xử lý.</li>
                    <li class="mb-1"><strong>Giám sát Tức thời:</strong> Xem nhanh biểu đồ KPI sau khi thực hiện tác động.</li>
                    <li><strong>Đóng vòng Tối ưu:</strong> Theo dõi sự cải thiện điểm QoE/QoS ở tuần tiếp theo.</li>
                </ol>
            </div>
            
            <div class="row mb-4">
                <div class="col-md-12">
                    <form method="GET" action="/optimize" class="row g-3 align-items-center bg-white p-3 rounded-3 border shadow-sm">
                        <div class="col-md-8">
                            <label class="form-label fw-bold small text-muted">CHỌN TUẦN PHÂN TÍCH (BƯỚC 1)</label>
                            <select name="week_name" class="form-select border-0 shadow-sm bg-light">
                                {% for w in all_weeks %}
                                <option value="{{ w }}" {% if w == latest_week %}selected{% endif %}>{{ w }}</option>
                                {% endfor %}
                            </select>
                        </div>
                        <div class="col-md-4 align-self-end d-flex gap-2">
                            <button type="submit" name="action" value="filter" class="btn btn-danger w-100 shadow-sm"><i class="fa-solid fa-filter me-1"></i>Lọc</button>
                            <button type="submit" name="action" value="export" class="btn btn-success w-100 shadow-sm"><i class="fa-solid fa-file-excel me-1"></i>Export</button>
                        </div>
                    </form>
                </div>
            </div>
            
            <div class="d-flex justify-content-between align-items-center mb-3 mt-4">
                <h6 class="fw-bold text-danger mb-0"><i class="fa-solid fa-list-check me-2"></i>Danh sách Trạm Cần Xử lý ({{ latest_week or 'Chưa có dữ liệu' }})</h6>
            </div>
            
            <div class="table-responsive bg-white rounded shadow-sm border" style="max-height: 65vh;">
                 <table class="table table-hover table-bordered mb-0 align-middle" style="font-size: 0.85rem;">
                     <thead class="table-light text-center position-sticky top-0" style="z-index: 10;">
                         <tr>
                             <th rowspan="2" class="align-middle">Cell Name</th>
                             <th colspan="2">1. Báo cáo Tuần (Macro)</th>
                             <th colspan="4">2. KPI Ngày Gần Nhất (Micro)</th>
                             <th rowspan="2" class="align-middle">3. Chẩn đoán (Bệnh)</th>
                             <th rowspan="2" class="align-middle">4. Giải pháp (Action)</th>
                             <th rowspan="2" class="align-middle">5. Giám sát</th>
                         </tr>
                         <tr>
                             <th>Điểm QoE</th><th>Điểm QoS</th>
                             <th>PRB (%)</th><th>Thput (Mbps)</th><th>CQI (%)</th><th>Drop (%)</th>
                         </tr>
                     </thead>
                     <tbody>
                         {% for row in optimized_data %}
                         <tr>
                             <td class="fw-bold text-primary text-nowrap">{{ row.cell_name }}</td>
                             <td class="text-center {{ 'text-danger fw-bold' if row.qoe_score != '-' and row.qoe_score <= 2 }}">{{ row.qoe_score }}{% if row.qoe_percent != '-' %}<br><small class="text-muted">({{ row.qoe_percent }}%)</small>{% endif %}</td>
                             <td class="text-center {{ 'text-danger fw-bold' if row.qos_score != '-' and row.qos_score <= 3 }}">{{ row.qos_score }}{% if row.qos_percent != '-' %}<br><small class="text-muted">({{ row.qos_percent }}%)</small>{% endif %}</td>
                             <td class="text-center {{ 'text-danger fw-bold' if row.prb != '-' and row.prb > 20 }}">{{ row.prb }}</td>
                             <td class="text-center {{ 'text-danger fw-bold' if row.thput != '-' and row.thput < 10 }}">{{ row.thput }}</td>
                             <td class="text-center {{ 'text-danger fw-bold' if row.cqi != '-' and row.cqi < 93 }}">{{ row.cqi }}</td>
                             <td class="text-center {{ 'text-danger fw-bold' if row.drop != '-' and row.drop > 0.3 }}">{{ row.drop }}</td>
                             <td>
                                 <ul class="mb-0 ps-3 text-danger fw-bold" style="min-width: 150px;">
                                     {% for issue in row.issues %}<li>{{ issue }}</li>{% endfor %}
                                 </ul>
                             </td>
                             <td>
                                 <ul class="mb-0 ps-3 text-success" style="min-width: 180px;">
                                     {% for action in row.actions %}<li>{{ action }}</li>{% endfor %}
                                 </ul>
                             </td>
                             <td class="text-center p-2" style="min-width: 100px;">
                                 <a href="/kpi?tech=4g&cell_name={{ row.cell_name }}" class="btn btn-sm btn-outline-primary mb-1 w-100 py-0 shadow-sm" style="font-size: 0.75rem;"><i class="fa-solid fa-chart-line me-1"></i>Xem KPI</a>
                                 <a href="/qoe-qos?cell_name={{ row.cell_name }}" class="btn btn-sm btn-outline-warning w-100 py-0 text-dark shadow-sm" style="font-size: 0.75rem;"><i class="fa-solid fa-star-half-stroke me-1"></i>Xem QoE</a>
                             </td>
                         </tr>
                         {% else %}
                         <tr><td colspan="10" class="text-center py-5 text-muted"><i class="fa-solid fa-face-smile fa-3x mb-3 text-success d-block"></i>Tuyệt vời! Không phát hiện Cell nào vi phạm ngưỡng tệ trong tuần gần nhất.</td></tr>
                         {% endfor %}
                     </tbody>
                 </table>
            </div>
        
        {% elif active_page == 'gis' %}
            <div class="row mb-4">
                <div class="col-md-12">
                    <form method="POST" action="/gis" enctype="multipart/form-data" class="row g-3 align-items-center bg-light p-3 rounded-3 border">
                        <div class="col-md-2"><label class="form-label fw-bold small text-muted">CÔNG NGHỆ</label><select name="tech" class="form-select border-0 shadow-sm"><option value="3g" {% if selected_tech == '3g' %}selected{% endif %}>3G</option><option value="4g" {% if selected_tech == '4g' %}selected{% endif %}>4G</option><option value="5g" {% if selected_tech == '5g' %}selected{% endif %}>5G</option></select></div>
                        <div class="col-md-2"><label class="form-label fw-bold small text-muted">SITE CODE</label><input type="text" name="site_code" class="form-control border-0 shadow-sm" placeholder="VD: THA001" value="{{ site_code_input }}"></div>
                        <div class="col-md-3"><label class="form-label fw-bold small text-muted">CELL NAME</label><input type="text" name="cell_name" class="form-control border-0 shadow-sm" placeholder="VD: THA001_1" value="{{ cell_name_input }}"></div>
                        <div class="col-md-3"><label class="form-label fw-bold small text-muted text-warning"><i class="fa-solid fa-file-lines me-1"></i>LOG ITS (.TXT/.CSV)</label><input type="file" name="its_file" class="form-control border-0 shadow-sm" accept=".txt,.csv" multiple></div>
                        <div class="col-md-2 d-flex flex-column gap-2 mt-4 pt-1"><button type="submit" name="action" value="search" class="btn btn-primary btn-sm w-100 shadow-sm fw-bold"><i class="fa-solid fa-search me-1"></i>Tìm kiếm</button><button type="submit" name="action" value="show_log" class="btn btn-warning btn-sm w-100 shadow-sm fw-bold text-white"><i class="fa-solid fa-route me-1"></i>Xem Log</button></div>
                    </form>
                </div>
            </div>
            <div class="card border border-light shadow-sm">
                <div class="card-body p-1 position-relative">
                    <div id="gisMap" style="height: 65vh; width: 100%; border-radius: 8px; z-index: 1;"></div>
                </div>
            </div>
            <script>
                document.addEventListener('DOMContentLoaded', function() {
                    var gisData = {{ gis_data | tojson | safe if gis_data else '[]' }};
                    var itsData = {{ its_data | tojson | safe if its_data else '[]' }};
                    var actionType = "{{ action_type }}";
                    var searchSite = "{{ site_code_input }}";
                    var searchCell = "{{ cell_name_input }}";
                    var isShowIts = {{ 'true' if show_its else 'false' }};
                    var hasGisData = gisData.length > 0;
                    var hasItsData = isShowIts && itsData.length > 0;
                    
                    if(!document.getElementById('gisMap')) return;

                    var mapCenter = [19.807, 105.776];
                    var mapZoom = 9;

                    var map = L.map('gisMap', {
                        center: mapCenter,
                        zoom: mapZoom,
                        zoomControl: true, // Bật zoom ở góc topleft
                        fullscreenControl: true, // Bật FullScreen
                        fullscreenControlOptions: { position: 'topleft' } // Chuyển lên góc topleft
                    });

                    var googleStreets = L.tileLayer('https://{s}.google.com/vt/lyrs=m&x={x}&y={y}&z={z}', {
                        maxZoom: 22,
                        subdomains: ['mt0', 'mt1', 'mt2', 'mt3'],
                        detectRetina: true,
                        attribution: '© Google Maps'
                    }).addTo(map);
                    
                    var googleHybrid = L.tileLayer('https://{s}.google.com/vt/lyrs=y&x={x}&y={y}&z={z}', {
                        maxZoom: 22,
                        subdomains: ['mt0', 'mt1', 'mt2', 'mt3'],
                        detectRetina: true,
                        attribution: '© Google Maps'
                    });

                    L.control.layers({"Bản đồ (Google)": googleStreets, "Vệ tinh (Google)": googleHybrid}).addTo(map);

                    // Add Custom Settings Control
                    if (hasGisData || hasItsData) {
                        var settingsControl = L.control({position: 'topright'});
                        settingsControl.onAdd = function (map) {
                            var div = L.DomUtil.create('div', 'info settings-control shadow-sm');
                            div.style.backgroundColor = 'rgba(255, 255, 255, 0.95)';
                            div.style.padding = '12px';
                            div.style.borderRadius = '8px';
                            div.style.border = '1px solid #dee2e6';
                            
                            L.DomEvent.disableClickPropagation(div);

                            var html = '<div class="d-flex flex-column gap-2">';

                            if (hasGisData) {
                                html += '<div class="d-flex align-items-center justify-content-between">';
                                html += '<label class="small fw-bold text-muted mb-0 me-2" style="white-space: nowrap;"><i class="fa-solid fa-wifi text-info me-1"></i>Bán kính quạt:</label>';
                                html += '<input type="range" class="form-range" id="sectorRadiusSlider" min="50" max="2000" step="50" value="350" style="width: 100px;">';
                                html += '<span id="sectorRadiusVal" class="small text-muted ms-2 fw-bold" style="min-width: 45px; text-align: right;">350m</span>';
                                html += '</div>';
                            }

                            if (hasItsData) {
                                html += '<div class="d-flex align-items-center justify-content-between">';
                                html += '<label class="small fw-bold text-muted mb-0 me-2" style="white-space: nowrap;"><i class="fa-solid fa-circle text-primary me-1"></i>Cỡ hạt Log:</label>';
                                html += '<input type="range" class="form-range" id="pointSizeSlider" min="1" max="10" value="3" style="width: 100px;">';
                                html += '</div>';

                                html += '<div class="d-flex align-items-center mt-1 pt-2 border-top">';
                                html += '<div class="form-check form-switch mb-0 w-100 d-flex align-items-center ps-0">';
                                html += '<label class="form-check-label small fw-bold text-muted me-3" for="showLinesToggle" style="cursor: pointer;"><i class="fa-solid fa-link text-success me-1"></i>Nối vệt tia</label>';
                                html += '<input class="form-check-input mt-0 ms-auto float-none" type="checkbox" id="showLinesToggle" checked style="cursor: pointer;">';
                                html += '</div>';
                                html += '</div>';
                            }

                            html += '</div>';
                            div.innerHTML = html;
                            return div;
                        };
                        settingsControl.addTo(map);
                    }

                    // Bounds Management
                    var bounds = [];
                    if (isShowIts && itsData.length > 0) {
                        itsData.forEach(function(pt) { bounds.push([pt.lat, pt.lon]); });
                    }
                    if (actionType === 'show_log' && gisData.length > 0) {
                        gisData.forEach(function(cell) { bounds.push([cell.lat, cell.lon]); });
                    }

                    if (actionType === 'search' && (searchSite || searchCell) && gisData.length > 0) {
                        var targetCell = gisData[0];
                        for (var i = 0; i < gisData.length; i++) {
                            var sCode = (gisData[i].site_code || "").toLowerCase();
                            var cName = (gisData[i].cell_name || "").toLowerCase();
                            var sInput = searchSite.toLowerCase();
                            var cInput = searchCell.toLowerCase();
                            if ((sInput && sCode.includes(sInput)) || (cInput && cName.includes(cInput))) { targetCell = gisData[i]; break; }
                        }
                        if (targetCell.lat && targetCell.lon) { map.setView([targetCell.lat, targetCell.lon], 15); } 
                        else { map.setView(mapCenter, mapZoom); }
                    } else if (bounds.length > 0) {
                        map.fitBounds(bounds, {padding: [30, 30], maxZoom: 16});
                    } else {
                        map.setView(mapCenter, mapZoom);
                    }

                    var siteLayerGroup = L.layerGroup().addTo(map);
                    var sectorLayerGroup = L.layerGroup().addTo(map);
                    var itsLayerGroup = L.layerGroup().addTo(map);
                    var cellLookup = {};
                    var renderedSites = {};

                    var techColors = {'3g': '#0078d4', '4g': '#107c10', '5g': '#ffaa44'};

                    function getSectorMidPoint(lat, lon, azimuth, distanceMeters) {
                        var latFactor = 111320;
                        var lonFactor = 111320 * Math.cos(lat * Math.PI / 180);
                        var radMap = (azimuth || 0) * Math.PI / 180;
                        var dx = (distanceMeters * Math.sin(radMap)) / lonFactor;
                        var dy = (distanceMeters * Math.cos(radMap)) / latFactor;
                        return [lat + dy, lon + dx];
                    }

                    function getSectorPolygon(lat, lon, azimuth, beamwidth, radiusMeters) {
                        var center = [lat, lon];
                        var points = [center];
                        var startAngle = azimuth - beamwidth / 2;
                        var endAngle = azimuth + beamwidth / 2;
                        var latFactor = 111320;
                        var lonFactor = 111320 * Math.cos(lat * Math.PI / 180);
                        for (var i = startAngle; i <= endAngle; i += 5) {
                            var radMap = i * Math.PI / 180;
                            var dx = (radiusMeters * Math.sin(radMap)) / lonFactor; 
                            var dy = (radiusMeters * Math.cos(radMap)) / latFactor; 
                            points.push([lat + dy, lon + dx]);
                        }
                        points.push(center);
                        return points;
                    }

                    gisData.forEach(function(cell) {
                        if(!cell.lat || !cell.lon) return;
                        if (!renderedSites[cell.site_code]) {
                            L.circleMarker([cell.lat, cell.lon], {radius: 5, color: '#333', weight: 1.5, fillColor: '#ffffff', fillOpacity: 1}).bindPopup("<b>Site Code:</b> " + cell.site_code).addTo(siteLayerGroup);
                            renderedSites[cell.site_code] = true;
                        }
                    });

                    function cVal(v) {
                        if (v === null || v === undefined) return "";
                        var s = String(v).trim();
                        if (s.endsWith(".0")) s = s.slice(0, -2);
                        return s;
                    }

                    function buildLookupAndDrawSectors() {
                        sectorLayerGroup.clearLayers();
                        cellLookup = {};

                        var radiusSlider = document.getElementById('sectorRadiusSlider');
                        var radiusSlider = document.getElementById('sectorRadiusSlider');
                        var sectorRadius = radiusSlider ? parseInt(radiusSlider.value) : 350;
                        var valDisplay = document.getElementById('sectorRadiusVal');
                        if (valDisplay) valDisplay.innerText = sectorRadius + 'm';

                        var targetCellCoord = null;
                        var targetPolygon = null;

                        gisData.forEach(function(cell) {
                            if(!cell.lat || !cell.lon) return;

                            var isMatch = false;
                            if (actionType === 'search' && (searchSite || searchCell)) {
                                var sCode = (cell.site_code || "").toLowerCase();
                                var cName = (cell.cell_name || "").toLowerCase();
                                var sInput = searchSite.toLowerCase();
                                var cInput = searchCell.toLowerCase();
                                if ((sInput && sCode.includes(sInput)) || (cInput && cName.includes(cInput))) { 
                                    isMatch = true;
                                    if (!targetCellCoord) targetCellCoord = [cell.lat, cell.lon];
                                }
                            }

                            var tech = cell.tech;
                            var info = cell.info;
                            var key = "";

                            if (tech === '4g') {
                                var en = cVal(info.enodeb_id); var lc = cVal(info.lcrid);
                                if (en && lc) key = en + "_" + lc;
                            } else if (tech === '3g') {
                                var ci = cVal(info.ci);
                                if (ci) key = ci;
                            } else if (tech === '5g') {
                                 var gn = cVal(info.gnodeb_id); var lc5 = cVal(info.lcrid);
                                 if (gn && lc5) key = gn + "_" + lc5;
                            }

                            if (key) {
                                cellLookup[key] = getSectorMidPoint(cell.lat, cell.lon, cell.azi, sectorRadius * 0.65);
                            }

                            var color = techColors[cell.tech] || '#dc3545';
                            var polyPoints = getSectorPolygon(cell.lat, cell.lon, cell.azi, 60, sectorRadius);
                            var polygon = L.polygon(polyPoints, {
                                color: isMatch ? '#ff0000' : color, 
                                weight: isMatch ? 3 : 1, 
                                fillColor: isMatch ? '#ff0000' : color, 
                                fillOpacity: isMatch ? 0.7 : 0.35
                            }).addTo(sectorLayerGroup);

                            if (isMatch && !targetPolygon) targetPolygon = polygon;

                            var infoHtml = "<div style='max-height: 250px; overflow-y: auto; overflow-x: hidden;'><table class='table table-sm table-bordered mb-0' style='font-size: 0.8rem;'>";
                            for (const [k, v] of Object.entries(cell.info)) {
                                if (v !== null && v !== '' && v !== 'None') {
                                    infoHtml += "<tr><th class='text-muted bg-light w-50'>" + k.toUpperCase() + "</th><td class='fw-bold'>" + v + "</td></tr>";
                                }
                            }
                            infoHtml += "</table></div>";

                            polygon.bindPopup(
                                "<div class='mb-2 pb-2 border-bottom'><b>Cell:</b> <span class='text-primary fs-6'>" + cell.cell_name + "</span><br>" +
                                "<b>Site:</b> " + cell.site_code + "<br>" +
                                "<b>Tọa độ:</b> " + cell.lat + ", " + cell.lon + "</div>" + infoHtml,
                                { minWidth: 300, maxWidth: 450 }
                            );
                        });
                        
                        if (actionType === 'search' && targetCellCoord) {
                            map.setView(targetCellCoord, 16);
                            if (targetPolygon) setTimeout(() => targetPolygon.openPopup(), 600);
                        }

                        drawITSData();
                    }

                    function getSignalColor(tech, level) {
                        var t = (tech || '').toUpperCase();
                        if (t.includes('4G') || t.includes('LTE')) {
                            if (level >= -75) return '#0000FF'; // Blue
                            if (level >= -85) return '#00FF00'; // Green
                            if (level >= -95) return '#FFFF00'; // Yellow
                            if (level >= -105) return '#FFA500'; // Orange
                            if (level >= -115) return '#FF0000'; // Red
                            return '#000000'; // Black
                        } else { 
                            if (level >= -65) return '#0000FF'; // Blue
                            if (level >= -75) return '#00FF00'; // Green
                            if (level >= -85) return '#FFFF00'; // Yellow
                            if (level >= -95) return '#FFA500'; // Orange
                            if (level >= -105) return '#FF0000'; // Red
                            return '#000000'; // Black
                        }
                    }

                    function drawITSData() {
                        if (!isShowIts || itsData.length === 0) return;
                        itsLayerGroup.clearLayers();

                        var pointSizeSlider = document.getElementById('pointSizeSlider');
                        var pointSize = pointSizeSlider ? parseInt(pointSizeSlider.value) : 3;
                        var showLinesToggle = document.getElementById('showLinesToggle');
                        var showLines = showLinesToggle ? showLinesToggle.checked : false;

                        itsData.forEach(function(pt) {
                            var ptColor = getSignalColor(pt.tech, pt.level);
                            var ptCoord = [pt.lat, pt.lon];
                            
                            L.circleMarker(ptCoord, {radius: pointSize, fillColor: ptColor, color: "#000", weight: 0.5, fillOpacity: 0.9})
                            .bindPopup("<div class='small'><b>Tech:</b> " + pt.tech + "<br><b>Level:</b> <span class='fw-bold' style='color:"+ptColor+"'>" + pt.level + " dBm</span><br><b>Qual:</b> " + (pt.qual||'-') + "<br><b>Node/CellID:</b> " + (pt.node||'-') + " / " + pt.cellid + "</div>")
                            .addTo(itsLayerGroup);

                            if (showLines) {
                                var ptTech = (pt.tech || '').toLowerCase();
                                var key = "";
                                if (ptTech.includes('4g') || ptTech.includes('lte')) {
                                    if (pt.node && pt.cellid) key = pt.node + "_" + pt.cellid;
                                } else if (ptTech.includes('3g') || ptTech.includes('wcdma') || ptTech.includes('hspa') || ptTech.includes('umts')) {
                                    if (pt.cellid) key = pt.cellid;
                                } else if (ptTech.includes('5g') || ptTech.includes('nr')) {
                                    if (pt.node && pt.cellid) key = pt.node + "_" + pt.cellid;
                                }

                                var targetCellCoord = cellLookup[key];
                                if (targetCellCoord) {
                                    L.polyline([ptCoord, targetCellCoord], {color: ptColor, weight: 1.5, opacity: 0.6, dashArray: '3, 4'}).addTo(itsLayerGroup);
                                }
                            }
                        });
                    }

                    buildLookupAndDrawSectors();

                    var radSliderEl = document.getElementById('sectorRadiusSlider');
                    if (radSliderEl) radSliderEl.addEventListener('input', buildLookupAndDrawSectors);

                    var sliderEl = document.getElementById('pointSizeSlider');
                    if (sliderEl) sliderEl.addEventListener('input', drawITSData);

                    var toggleEl = document.getElementById('showLinesToggle');
                    if (toggleEl) toggleEl.addEventListener('change', drawITSData);

                    // Add Legend
                    if (isShowIts && itsData.length > 0) {
                        var legend = L.control({position: 'bottomright'});
                        legend.onAdd = function (map) {
                            var div = L.DomUtil.create('div', 'info legend shadow-sm');
                            div.style.backgroundColor = 'rgba(255, 255, 255, 0.95)';
                            div.style.padding = '10px 15px';
                            div.style.borderRadius = '8px';
                            div.style.fontSize = '0.85rem';
                            div.style.lineHeight = '1.8';
                            div.style.border = '1px solid #dee2e6';
                            
                            var html = '';
                            var has4G = itsData.some(pt => (pt.tech || '').toUpperCase().includes('4G') || (pt.tech || '').toUpperCase().includes('LTE'));
                            var has3G = itsData.some(pt => !(pt.tech || '').toUpperCase().includes('4G') && !(pt.tech || '').toUpperCase().includes('LTE'));

                            if (has4G) {
                                html += '<strong class="text-primary fs-6 d-block mb-2"><i class="fa-solid fa-signal me-1"></i> Chú giải 4G RSRP</strong>';
                                html += '<div><i style="background:#0000FF; width:16px; height:16px; display:inline-block; margin-right:8px; border-radius:3px;"></i> Rất tốt (≥ -75)</div>';
                                html += '<div><i style="background:#00FF00; width:16px; height:16px; display:inline-block; margin-right:8px; border-radius:3px;"></i> Tốt (-85 đến -75)</div>';
                                html += '<div><i style="background:#FFFF00; width:16px; height:16px; display:inline-block; margin-right:8px; border-radius:3px;"></i> Khá (-95 đến -85)</div>';
                                html += '<div><i style="background:#FFA500; width:16px; height:16px; display:inline-block; margin-right:8px; border-radius:3px;"></i> Kém (-105 đến -95)</div>';
                                html += '<div><i style="background:#FF0000; width:16px; height:16px; display:inline-block; margin-right:8px; border-radius:3px;"></i> Rất kém (-115 đến -105)</div>';
                                html += '<div><i style="background:#000000; width:16px; height:16px; display:inline-block; margin-right:8px; border-radius:3px;"></i> Mất sóng (< -115)</div>';
                            }
                            if (has3G) {
                                if (has4G) html += '<hr class="my-2">';
                                html += '<strong class="text-success fs-6 d-block mb-2"><i class="fa-solid fa-signal me-1"></i> Chú giải 3G RSCP</strong>';
                                html += '<div><i style="background:#0000FF; width:16px; height:16px; display:inline-block; margin-right:8px; border-radius:3px;"></i> Rất tốt (≥ -65)</div>';
                                html += '<div><i style="background:#00FF00; width:16px; height:16px; display:inline-block; margin-right:8px; border-radius:3px;"></i> Tốt (-75 đến -65)</div>';
                                html += '<div><i style="background:#FFFF00; width:16px; height:16px; display:inline-block; margin-right:8px; border-radius:3px;"></i> Khá (-85 đến -75)</div>';
                                html += '<div><i style="background:#FFA500; width:16px; height:16px; display:inline-block; margin-right:8px; border-radius:3px;"></i> Kém (-95 đến -85)</div>';
                                html += '<div><i style="background:#FF0000; width:16px; height:16px; display:inline-block; margin-right:8px; border-radius:3px;"></i> Rất kém (-105 đến -95)</div>';
                                html += '<div><i style="background:#000000; width:16px; height:16px; display:inline-block; margin-right:8px; border-radius:3px;"></i> Mất sóng (< -105)</div>';
                            }
                            div.innerHTML = html;
                            return div;
                        };
                        legend.addTo(map);
                    }
                });
            </script>

        {% elif active_page == 'kpi' %}
            <div class="row mb-4">
                <div class="col-md-12">
                    <form method="GET" action="/kpi" class="row g-3 align-items-center bg-light p-3 rounded-3 border">
                        <div class="col-md-2"><label class="form-label fw-bold small text-muted">CÔNG NGHỆ</label><select name="tech" class="form-select border-0 shadow-sm"><option value="3g" {% if selected_tech == '3g' %}selected{% endif %}>3G</option><option value="4g" {% if selected_tech == '4g' %}selected{% endif %}>4G</option><option value="5g" {% if selected_tech == '5g' %}selected{% endif %}>5G</option></select></div>
                        <div class="col-md-4"><label class="form-label fw-bold small text-muted">TÌM THEO POI</label><input type="text" name="poi_name" list="poi_list_kpi" class="form-control border-0 shadow-sm" placeholder="Chọn POI..." value="{{ selected_poi }}"><datalist id="poi_list_kpi">{% for p in poi_list %}<option value="{{ p }}">{% endfor %}</datalist></div>
                        <div class="col-md-3"><label class="form-label fw-bold small text-muted">NHẬP CELL/SITE</label><input type="text" name="cell_name" class="form-control border-0 shadow-sm" placeholder="Site code, Cell list..." value="{{ cell_name_input }}"></div>
                        <div class="col-md-2 align-self-end"><button type="submit" class="btn btn-primary w-100 shadow-sm">Visualize</button></div>
                    </form>
                </div>
            </div>
            {% if charts %}
                {% for chart_id, chart_config in charts.items() %}
                <div class="card mb-4 border-0 shadow-sm"><div class="card-body p-4"><h6 class="card-title text-secondary fw-bold mb-3">{{ chart_config.title }}</h6><div class="chart-container" style="position: relative; height:45vh; width:100%"><canvas id="{{ chart_id }}"></canvas></div></div></div>
                {% endfor %}
                <script>{% for chart_id, chart_data in charts.items() %}(function(){const ctx=document.getElementById('{{ chart_id }}').getContext('2d'); const cd={{ chart_data | tojson }}; new Chart(ctx,{type:'line',data:cd,options:{responsive:true,maintainAspectRatio:false,spanGaps:true,elements:{line:{tension:0.3}},interaction:{mode:'nearest',intersect:false,axis:'x'},onClick:(e,el)=>{if(el.length>0){const i=el[0].index;const di=el[0].datasetIndex;showDetailModal(cd.datasets[di].label,cd.labels[i],cd.datasets[di].data[i],'{{ chart_data.title }}',cd.datasets,cd.labels);}},plugins:{legend:{position:'bottom'},tooltip:{mode:'index',intersect:false}}}});})();{% endfor %}</script>
            {% elif cell_name_input or selected_poi %}
                <div class="alert alert-warning border-0 shadow-sm"><i class="fa-solid fa-circle-exclamation me-2"></i>Không tìm thấy dữ liệu phù hợp.</div>
            {% else %}
                <div class="text-center text-muted py-5 opacity-50"><i class="fa-solid fa-chart-line fa-4x mb-3"></i><p class="fs-5">Vui lòng chọn tiêu chí để xem báo cáo KPI.</p></div>
            {% endif %}

        {% elif active_page == 'qoe_qos' %}
            <div class="row mb-4">
                <div class="col-md-12">
                    <form method="GET" action="/qoe-qos" class="row g-3 align-items-center bg-light p-3 rounded-3 border">
                        <div class="col-md-6"><label class="form-label fw-bold small text-muted">NHẬP CELL NAME 4G</label><input type="text" name="cell_name" class="form-control border-0 shadow-sm" placeholder="VD: THA001_1" value="{{ cell_name_input }}" required></div>
                        <div class="col-md-2 align-self-end"><button type="submit" class="btn btn-primary w-100 shadow-sm"><i class="fa-solid fa-search me-2"></i>Tra cứu</button></div>
                        {% if has_data %}<div class="col-md-3 align-self-end"><a href="/kpi?tech=4g&cell_name={{ cell_name_input }}" class="btn btn-success w-100 shadow-sm"><i class="fa-solid fa-link me-2"></i>Link tới KPI Cell</a></div>{% endif %}
                    </form>
                </div>
            </div>
            {% if charts %}
                <div class="row">
                    {% for chart_id, c in charts.items() %}
                    <div class="col-md-6 mb-4"><div class="card h-100 border-0 shadow-sm"><div class="card-body p-4"><h6 class="card-title text-secondary fw-bold mb-3">{{ c.title }}</h6><div class="chart-container" style="position: relative; height:35vh; width:100%"><canvas id="{{ chart_id }}"></canvas></div></div></div></div>
                    <script>(function(){ const cd={{ c | tojson }}; new Chart(document.getElementById('{{ chart_id }}').getContext('2d'),{type:'line',data:cd,options:{responsive:true,maintainAspectRatio:false,spanGaps:true,elements:{line:{tension:0.3}},interaction:{mode:'nearest',intersect:false,axis:'x'},onClick:(e,el)=>{if(el.length>0)showDetailModal(cd.datasets[el[0].datasetIndex].label,cd.labels[el[0].index],cd.datasets[el[0].datasetIndex].data[el[0].index],'{{ c.title }}',cd.datasets,cd.labels)}}});})();</script>
                    {% endfor %}
                </div>
                {% if qoe_details %}
                <div class="card mt-2 shadow-sm border-0 mb-4"><div class="card-header bg-white fw-bold text-primary">Dữ liệu gốc QoE Hàng tuần</div><div class="card-body p-0 table-responsive"><table class="table table-bordered table-striped table-hover mb-0 text-nowrap" style="font-size: 0.8rem;"><thead class="table-light"><tr><th>Tuần</th>{% for k in qoe_headers %}<th>{{ k }}</th>{% endfor %}</tr></thead><tbody>{% for row in qoe_details %}<tr><td class="fw-bold text-primary">{{ row.week }}</td>{% for k in qoe_headers %}<td>{{ row.data.get(k, '-') }}</td>{% endfor %}</tr>{% endfor %}</tbody></table></div></div>
                {% endif %}
                {% if qos_details %}
                <div class="card shadow-sm border-0 mb-4"><div class="card-header bg-white fw-bold text-success">Dữ liệu gốc QoS Hàng tuần</div><div class="card-body p-0 table-responsive"><table class="table table-bordered table-striped table-hover mb-0 text-nowrap" style="font-size: 0.8rem;"><thead class="table-light"><tr><th>Tuần</th>{% for k in qos_headers %}<th>{{ k }}</th>{% endfor %}</tr></thead><tbody>{% for row in qos_details %}<tr><td class="fw-bold text-success">{{ row.week }}</td>{% for k in qos_headers %}<td>{{ row.data.get(k, '-') }}</td>{% endfor %}</tr>{% endfor %}</tbody></table></div></div>
                {% endif %}
            {% elif cell_name_input %}
                <div class="alert alert-warning border-0 shadow-sm"><i class="fa-solid fa-circle-exclamation me-2"></i>Không tìm thấy dữ liệu QoE/QoS cho Cell: <strong>{{ cell_name_input }}</strong>.</div>
            {% else %}
                <div class="text-center text-muted py-5"><i class="fa-solid fa-star-half-stroke fa-3x mb-3 opacity-50"></i><p>Nhập mã Cell 4G để xem biểu đồ xu hướng QoE, QoS.</p></div>
            {% endif %}

        {% elif active_page == 'poi' %}
            <div class="row mb-4">
                <div class="col-md-12">
                    <form method="GET" action="/poi" class="row g-3 align-items-center bg-light p-3 rounded-3 border">
                        <div class="col-md-8"><label class="form-label fw-bold small text-muted">CHỌN POI</label><input type="text" name="poi_name" list="poi_list_kpi" class="form-control border-0 shadow-sm" placeholder="Chọn POI..." value="{{ selected_poi }}"><datalist id="poi_list_kpi">{% for p in poi_list %}<option value="{{ p }}">{% endfor %}</datalist></div>
                        <div class="col-md-4 align-self-end"><button type="submit" class="btn btn-primary w-100 shadow-sm">Xem Báo Cáo</button></div>
                    </form>
                </div>
            </div>
            {% if poi_charts %}
                <div class="row">
                    {% for chart_id, c in poi_charts.items() %}
                    <div class="col-md-6 mb-4"><div class="card h-100 border-0 shadow-sm"><div class="card-body p-4"><h6 class="card-title text-secondary fw-bold mb-3">{{ c.title }}</h6><div class="chart-container" style="position: relative; height:35vh; width:100%"><canvas id="{{ chart_id }}"></canvas></div></div></div></div>
                    <script>(function(){ const cd={{ c | tojson }}; new Chart(document.getElementById('{{ chart_id }}').getContext('2d'),{type:'line',data:cd,options:{responsive:true,maintainAspectRatio:false,spanGaps:true,elements:{line:{tension:0.3}},interaction:{mode:'nearest',intersect:false,axis:'x'},onClick:(e,el)=>{if(el.length>0)showDetailModal(cd.datasets[el[0].datasetIndex].label,cd.labels[el[0].index],cd.datasets[el[0].datasetIndex].data[el[0].index],'{{ c.title }}',cd.datasets,cd.labels)}}});})();</script>
                    {% endfor %}
                </div>
            {% elif selected_poi %}
                <div class="alert alert-warning border-0 shadow-sm">Không có dữ liệu cho POI: <strong>{{ selected_poi }}</strong></div>
            {% else %}
                <div class="text-center text-muted py-5"><i class="fa-solid fa-map-location-dot fa-3x mb-3"></i><p>Chọn địa điểm POI để xem báo cáo.</p></div>
            {% endif %}

        {% elif active_page == 'worst_cell' %}
            <div class="row mb-4"><div class="col-md-12"><form method="GET" action="/worst-cell" class="row g-3 align-items-center bg-light p-3 rounded-3 border"><div class="col-auto"><label class="col-form-label fw-bold text-muted">THỜI GIAN</label></div><div class="col-auto"><select name="duration" class="form-select border-0 shadow-sm"><option value="1" {% if duration == 1 %}selected{% endif %}>1 ngày mới nhất</option><option value="3" {% if duration == 3 %}selected{% endif %}>3 ngày liên tiếp</option><option value="7" {% if duration == 7 %}selected{% endif %}>7 ngày liên tiếp</option><option value="15" {% if duration == 15 %}selected{% endif %}>15 ngày liên tiếp</option><option value="30" {% if duration == 30 %}selected{% endif %}>30 ngày liên tiếp</option></select></div><div class="col-auto"><button type="submit" name="action" value="execute" class="btn btn-danger shadow-sm">Lọc Worst Cell</button></div><div class="col-auto"><button type="submit" name="action" value="export" class="btn btn-success shadow-sm ms-2"><i class="fa-solid fa-file-excel me-2"></i>Export Excel</button></div></form></div></div>
            {% if dates %}<div class="alert alert-info border-0 shadow-sm mb-4"><i class="fa-solid fa-calendar-days me-2"></i><strong>Xét duyệt:</strong> {% for d in dates %}<span class="badge bg-white text-info border ms-1">{{ d }}</span>{% endfor %}</div>{% endif %}
            <div class="table-responsive bg-white rounded shadow-sm border" style="max-height: 70vh;">
                <table class="table table-hover mb-0" style="font-size: 0.9rem;"><thead class="bg-light position-sticky top-0" style="z-index: 10;"><tr><th>Cell Name</th><th class="text-center">Avg Thput</th><th class="text-center">Avg PRB</th><th class="text-center">Avg CQI</th><th class="text-center">Avg Drop Rate</th><th class="text-center">Hành động</th></tr></thead><tbody>{% for r in worst_cells %}<tr><td class="fw-bold text-primary">{{ r.cell_name }}</td><td class="text-center {{ 'text-danger fw-bold' if r.avg_thput < 7000 }}">{{ r.avg_thput }}</td><td class="text-center {{ 'text-danger fw-bold' if r.avg_res_blk > 20 }}">{{ r.avg_res_blk }}</td><td class="text-center {{ 'text-danger fw-bold' if r.avg_cqi < 93 }}">{{ r.avg_cqi }}</td><td class="text-center {{ 'text-danger fw-bold' if r.avg_drop > 0.3 }}">{{ r.avg_drop }}</td><td class="text-center"><a href="/kpi?tech=4g&cell_name={{ r.cell_name }}" class="btn btn-sm btn-success text-white">View</a></td></tr>{% else %}<tr><td colspan="6" class="text-center py-5 text-muted">Nhấn "Lọc Worst Cell" để xem dữ liệu</td></tr>{% endfor %}</tbody></table>
            </div>

        {% elif active_page == 'traffic_down' %}
             <div class="row mb-4"><div class="col-md-12"><form method="GET" action="/traffic-down" class="row g-3 align-items-center bg-light p-3 rounded-3 border"><div class="col-auto"><label class="col-form-label fw-bold text-muted">CÔNG NGHỆ:</label></div><div class="col-auto"><select name="tech" class="form-select border-0 shadow-sm"><option value="3g" {% if tech == '3g' %}selected{% endif %}>3G</option><option value="4g" {% if tech == '4g' %}selected{% endif %}>4G</option><option value="5g" {% if tech == '5g' %}selected{% endif %}>5G</option></select></div><div class="col-auto"><button type="submit" name="action" value="execute" class="btn btn-primary shadow-sm">Thực hiện</button><button type="submit" name="action" value="export_zero" class="btn btn-success shadow-sm ms-2"><i class="fa-solid fa-file-excel"></i> Zero</button><button type="submit" name="action" value="export_degraded" class="btn btn-success shadow-sm ms-2"><i class="fa-solid fa-file-excel"></i> Degraded</button><button type="submit" name="action" value="export_poi_degraded" class="btn btn-warning shadow-sm ms-2"><i class="fa-solid fa-file-excel"></i> POI Degraded</button></div><div class="col-auto ms-auto"><span class="badge bg-info text-dark">Ngày phân tích: {{ analysis_date }}</span></div></form></div></div>
            <div class="row g-4">
                <div class="col-md-4"><div class="card h-100 border-0 shadow-sm"><div class="card-header bg-danger text-white fw-bold">Cell Không Lưu Lượng (< 0.1 GB)</div><div class="card-body p-0 table-responsive"><table class="table table-striped mb-0 small"><thead class="table-light"><tr><th>Cell Name</th><th class="text-end">Today</th><th class="text-end">Avg (7 Days)</th><th class="text-center">Action</th></tr></thead><tbody>{% for r in zero_traffic %}<tr><td class="fw-bold">{{ r.cell_name }}</td><td class="text-end text-danger">{{ r.traffic_today }}</td><td class="text-end">{{ r.avg_last_7 }}</td><td class="text-center"><a href="/kpi?tech={{ tech }}&cell_name={{ r.cell_name }}" class="btn btn-xs btn-outline-primary"><i class="fa-solid fa-chart-line"></i></a></td></tr>{% endfor %}</tbody></table></div></div></div>
                <div class="col-md-4"><div class="card h-100 border-0 shadow-sm"><div class="card-header bg-warning text-dark fw-bold">Cell Suy Giảm (> 30%)</div><div class="card-body p-0 table-responsive"><table class="table table-striped mb-0 small"><thead class="table-light"><tr><th>Cell Name</th><th class="text-end">Today</th><th class="text-end">Last Week</th><th class="text-end">Degrade %</th><th class="text-center">Action</th></tr></thead><tbody>{% for r in degraded %}<tr><td class="fw-bold">{{ r.cell_name }}</td><td class="text-end text-danger">{{ r.traffic_today }}</td><td class="text-end">{{ r.traffic_last_week }}</td><td class="text-end text-danger fw-bold">-{{ r.degrade_percent }}%</td><td class="text-center"><a href="/kpi?tech={{ tech }}&cell_name={{ r.cell_name }}" class="btn btn-xs btn-outline-primary"><i class="fa-solid fa-chart-line"></i></a></td></tr>{% endfor %}</tbody></table></div></div></div>
                 <div class="col-md-4"><div class="card h-100 border-0 shadow-sm"><div class="card-header bg-warning text-dark fw-bold">POI Suy Giảm (> 30%)</div><div class="card-body p-0 table-responsive"><table class="table table-striped mb-0 small"><thead class="table-light"><tr><th>POI Name</th><th class="text-end">Today</th><th class="text-end">Last Week</th><th class="text-end">Degrade %</th><th class="text-center">Action</th></tr></thead><tbody>{% for r in degraded_pois %}<tr><td class="fw-bold">{{ r.poi_name }}</td><td class="text-end text-danger">{{ r.traffic_today }}</td><td class="text-end">{{ r.traffic_last_week }}</td><td class="text-end text-danger fw-bold">-{{ r.degrade_percent }}%</td><td class="text-center"><a href="/poi?poi_name={{ r.poi_name }}" class="btn btn-xs btn-outline-primary"><i class="fa-solid fa-chart-line"></i></a></td></tr>{% endfor %}</tbody></table></div></div></div>
            </div>

        {% elif active_page == 'conges_3g' %}
            <div class="row mb-4"><div class="col-md-12"><form method="GET" action="/conges-3g" class="d-flex align-items-center"><div class="alert alert-info border-0 shadow-sm bg-soft-primary text-primary mb-0 flex-grow-1"><strong>Điều kiện:</strong> (CS_CONG > 2% & CS_ATT > 100) OR (PS_CONG > 2% & PS_ATT > 500) (3 ngày liên tiếp)</div><button type="submit" name="action" value="execute" class="btn btn-primary shadow-sm ms-3">Thực hiện</button><button type="submit" name="action" value="export" class="btn btn-success shadow-sm ms-2"><i class="fa-solid fa-file-excel me-2"></i>Export</button></form></div></div>
            {% if dates %}<div class="mb-3 text-muted small"><i class="fa-solid fa-calendar me-2"></i>Xét duyệt: {% for d in dates %}<span class="badge bg-light text-dark border ms-1">{{ d }}</span>{% endfor %}</div>{% endif %}
            <div class="table-responsive bg-white rounded shadow-sm border"><table class="table table-hover mb-0" style="font-size: 0.9rem;"><thead class="bg-light"><tr><th>Cell Name</th><th>Avg CS Traffic</th><th>Avg CS Conges (%)</th><th>Avg PS Traffic</th><th>Avg PS Conges (%)</th><th class="text-center">Hành động</th></tr></thead><tbody>{% for r in conges_data %}<tr><td class="fw-bold text-primary">{{ r.cell_name }}</td><td>{{ r.avg_cs_traffic }}</td><td class="{{ 'text-danger fw-bold' if r.avg_cs_conges > 2 }}">{{ r.avg_cs_conges }}</td><td>{{ r.avg_ps_traffic }}</td><td class="{{ 'text-danger fw-bold' if r.avg_ps_conges > 2 }}">{{ r.avg_ps_conges }}</td><td class="text-center"><a href="/kpi?tech=3g&cell_name={{ r.cell_name }}" class="btn btn-sm btn-success text-white shadow-sm">View</a></td></tr>{% else %}<tr><td colspan="6" class="text-center py-5 text-muted opacity-50">Nhấn nút "Thực hiện" để xem kết quả</td></tr>{% endfor %}</tbody></table></div>

        {% elif active_page == 'rf' %}
             <div class="d-flex flex-wrap justify-content-between align-items-center mb-4 bg-white p-3 rounded shadow-sm border gap-3">
                 <div class="btn-group shadow-sm">
                     <a href="/rf?tech=3g" class="btn {{ 'btn-primary' if current_tech == '3g' else 'btn-outline-primary' }}">3G</a>
                     <a href="/rf?tech=4g" class="btn {{ 'btn-primary' if current_tech == '4g' else 'btn-outline-primary' }}">4G</a>
                     <a href="/rf?tech=5g" class="btn {{ 'btn-primary' if current_tech == '5g' else 'btn-outline-primary' }}">5G</a>
                 </div>
                 
                 <form method="GET" action="/rf" class="d-flex flex-grow-1 mx-lg-4">
                     <input type="hidden" name="tech" value="{{ current_tech }}">
                     <div class="input-group shadow-sm">
                         <span class="input-group-text bg-light border-end-0"><i class="fa-solid fa-search text-muted"></i></span>
                         <input type="text" name="cell_search" class="form-control border-start-0 ps-0" placeholder="Nhập Cell Code hoặc Site Code để tìm nhanh..." value="{{ search_query }}">
                         <button type="submit" class="btn btn-primary px-4 fw-bold">Tìm kiếm</button>
                     </div>
                 </form>

                 <div class="d-flex gap-2">
                     <form method="GET" action="/rf" class="m-0">
                         <input type="hidden" name="tech" value="{{ current_tech }}">
                         <input type="hidden" name="cell_search" value="{{ search_query }}">
                         <button type="submit" name="action" value="export" class="btn btn-success shadow-sm text-white fw-bold"><i class="fa-solid fa-file-excel me-2"></i>Export</button>
                     </form>
                     {% if current_user.role == 'admin' %}
                     <a href="/rf/add?tech={{ current_tech }}" class="btn btn-warning shadow-sm fw-bold"><i class="fa-solid fa-plus me-1"></i>New</a>
                     {% endif %}
                 </div>
             </div>
             
             <div class="table-responsive bg-white rounded shadow-sm border" style="max-height: 65vh;">
                 <table class="table table-hover mb-0" style="font-size: 0.85rem; white-space: nowrap;">
                     <thead class="table-light position-sticky top-0" style="z-index: 10;">
                         <tr>
                             <th class="text-center border-bottom bg-light" style="position: sticky; left: 0; z-index: 20;">Action</th>
                             {% for col in rf_columns %}<th>{{ col | replace('_', ' ') | upper }}</th>{% endfor %}
                         </tr>
                     </thead>
                     <tbody>
                         {% for row in rf_data %}
                         <tr>
                             <td class="text-center bg-white border-end shadow-sm" style="position: sticky; left: 0; z-index: 5;">
                                 <a href="/rf/detail/{{ current_tech }}/{{ row['id'] }}" class="btn btn-sm btn-outline-primary py-0"><i class="fa-solid fa-eye"></i></a>
                                 {% if current_user.role == 'admin' %}
                                 <a href="/rf/edit/{{ current_tech }}/{{ row['id'] }}" class="btn btn-sm btn-outline-warning py-0"><i class="fa-solid fa-pen"></i></a>
                                 <a href="/rf/delete/{{ current_tech }}/{{ row['id'] }}" class="btn btn-sm btn-outline-danger py-0" onclick="return confirm('Xóa?')"><i class="fa-solid fa-trash"></i></a>
                                 {% endif %}
                             </td>
                             {% for col in rf_columns %}<td>{{ row[col] }}</td>{% endfor %}
                         </tr>
                         {% else %}
                         <tr><td colspan="100%" class="text-center py-4 text-muted"><i class="fa-solid fa-magnifying-glass fa-2x mb-2 d-block opacity-50"></i>Không tìm thấy trạm nào.</td></tr>
                         {% endfor %}
                     </tbody>
                 </table>
             </div>

        {% elif active_page == 'import' %}
             <div class="row">
                 <div class="col-md-8">
                     <div class="tab-content bg-white p-4 rounded-3 shadow-sm border">
                         <h5 class="mb-3 text-primary"><i class="fa-solid fa-cloud-arrow-up me-2"></i>Data Import</h5>
                         <ul class="nav nav-tabs mb-4" id="importTabs" role="tablist">
                             <li class="nav-item"><button class="nav-link active" data-bs-toggle="tab" data-bs-target="#tabRF" type="button">Import RF</button></li>
                             <li class="nav-item"><button class="nav-link" data-bs-toggle="tab" data-bs-target="#tabPOI" type="button">Import POI</button></li>
                             <li class="nav-item"><button class="nav-link" data-bs-toggle="tab" data-bs-target="#tabKPI" type="button">Import KPI</button></li>
                             <li class="nav-item"><button class="nav-link text-primary fw-bold" data-bs-toggle="tab" data-bs-target="#tabQoE" type="button">Import QoE/QoS</button></li>
                             <li class="nav-item"><button class="nav-link text-danger fw-bold" data-bs-toggle="tab" data-bs-target="#tabReset" type="button">Reset Data</button></li>
                         </ul>
                         <div class="tab-content">
                             <div class="tab-pane fade show active" id="tabRF">
                                 <div class="row">
                                     <!-- Cột 1: Form upload file -->
                                     <div class="col-md-7 border-end">
                                         <form action="/import" method="POST" enctype="multipart/form-data">
                                             <div class="mb-3">
                                                 <label class="form-label fw-bold">Chọn Loại Dữ Liệu RF</label>
                                                 <select name="type" class="form-select">
                                                     <option value="cell3g">1. CELL 3G (Thông số trạm, Tọa độ)</option>
                                                     <option value="config3g">2. CONFIG 3G (Thông số mạng)</option>
                                                     <option value="4g">RF 4G</option>
                                                     <option value="5g">RF 5G</option>
                                                 </select>
                                             </div>
                                             <div class="mb-3">
                                                 <label class="form-label fw-bold">Chọn File (.xlsx, .csv)</label>
                                                 <input type="file" name="file" class="form-control" multiple required>
                                             </div>
                                             <button class="btn btn-primary w-100"><i class="fa-solid fa-upload me-2"></i>Upload File</button>
                                         </form>
                                     </div>
                                     <!-- Cột 2: Nút đồng bộ riêng cho 3G -->
                                     <div class="col-md-5 d-flex flex-column justify-content-center px-4">
                                         <div class="alert alert-info py-2 small mb-3 border-0 shadow-sm">
                                             <i class="fa-solid fa-circle-info me-1"></i> Đối với 3G, bạn cần upload đủ 2 file (CELL 3G và CONFIG 3G), sau đó nhấn nút đồng bộ bên dưới.
                                         </div>
                                         <form action="/sync-rf3g" method="POST">
                                             <button type="submit" class="btn btn-success w-100 fw-bold shadow-sm py-3" onclick="return confirm('Bạn có chắc chắn muốn gộp dữ liệu Cell 3G và Config 3G vào bảng RF 3G chính không?');">
                                                 <i class="fa-solid fa-object-group me-2"></i>Ghép nối dữ liệu 3G<br><small class="fw-normal">(Sync to RF 3G)</small>
                                             </button>
                                         </form>
                                     </div>
                                 </div>
                             </div>
                             <div class="tab-pane fade" id="tabPOI">
                                 <form action="/import" method="POST" enctype="multipart/form-data">
                                     <div class="mb-3"><label class="form-label fw-bold">Chọn Loại Dữ Liệu POI</label><select name="type" class="form-select"><option value="poi4g">POI 4G</option><option value="poi5g">POI 5G</option></select></div>
                                     <div class="mb-3"><label class="form-label fw-bold">Chọn File (.xlsx, .csv)</label><input type="file" name="file" class="form-control" multiple required></div>
                                     <button class="btn btn-primary w-100"><i class="fa-solid fa-upload me-2"></i>Upload POI Data</button>
                                 </form>
                             </div>
                             <div class="tab-pane fade" id="tabKPI">
                                 <form action="/import" method="POST" enctype="multipart/form-data">
                                     <div class="mb-3"><label class="form-label fw-bold">Chọn Loại Dữ Liệu KPI</label><select name="type" class="form-select"><option value="kpi3g">KPI 3G</option><option value="kpi4g">KPI 4G</option><option value="kpi5g">KPI 5G</option></select></div>
                                     <div class="mb-3"><label class="form-label fw-bold">Chọn File (.xlsx, .csv)</label><input type="file" name="file" class="form-control" multiple required></div>
                                     <button class="btn btn-primary w-100"><i class="fa-solid fa-upload me-2"></i>Upload KPI Data</button>
                                 </form>
                             </div>
                             <div class="tab-pane fade" id="tabQoE">
                                 <form action="/import" method="POST" enctype="multipart/form-data">
                                     <div class="mb-3"><label class="form-label fw-bold text-primary">Chọn Loại Dữ Liệu QoE/QoS</label><select name="type" class="form-select border-primary"><option value="qoe4g">QoE 4G (Hàng Tuần)</option><option value="qos4g">QoS 4G (Hàng Tuần)</option></select></div>
                                     <div class="mb-3"><label class="form-label fw-bold">Tên Tuần (Quan trọng)</label><input type="text" name="week_name" class="form-control" value="{{ default_week_name }}" required></div>
                                     <div class="mb-3"><label class="form-label fw-bold">Chọn File (.xlsx, .csv)</label><input type="file" name="file" class="form-control" multiple required></div>
                                     <button class="btn btn-primary w-100"><i class="fa-solid fa-upload me-2"></i>Upload Data</button>
                                 </form>
                             </div>
                             <div class="tab-pane fade" id="tabReset">
                                 <div class="alert alert-warning border-0 shadow-sm mb-4">
                                     <i class="fa-solid fa-triangle-exclamation me-2"></i><strong>Cảnh báo:</strong> Hệ thống đã được cấu trúc lại bảng dữ liệu. BẠN CẦN NHẤN NÚT RESET NÀY TRƯỚC KHI IMPORT (nếu trước đó đã bị lỗi Unknown Column).
                                 </div>
                                 <div class="d-flex flex-column gap-3">
                                     <form action="/reset-data" method="POST" onsubmit="return confirm('CẢNH BÁO: Hành động này sẽ Drop và Create lại cấu trúc 5 bảng RF (3G, 4G, 5G). Dữ liệu cũ sẽ mất hoàn toàn. Bạn có chắc chắn?');">
                                         <input type="hidden" name="target" value="rf">
                                         <button type="submit" class="btn btn-danger w-100 shadow-sm fw-bold"><i class="fa-solid fa-trash-can me-2"></i>Reset Toàn Bộ Dữ Liệu RF (Cập nhật Schema)</button>
                                     </form>
                                     <form action="/reset-data" method="POST" onsubmit="return confirm('Bạn có chắc chắn muốn xóa sạch toàn bộ dữ liệu địa điểm POI của 4G, 5G?');">
                                         <input type="hidden" name="target" value="poi">
                                         <button type="submit" class="btn btn-danger w-100 shadow-sm fw-bold"><i class="fa-solid fa-trash-can me-2"></i>Reset Toàn Bộ Dữ Liệu POI</button>
                                     </form>
                                 </div>
                             </div>
                         </div>
                     </div>
                 </div>
                 <div class="col-md-4">
                     <div class="card h-100 border-0 shadow-sm"><div class="card-header bg-white fw-bold text-success border-bottom">Data History</div><div class="card-body p-0 overflow-auto" style="max-height: 400px;"><table class="table table-sm table-striped mb-0 text-center"><thead class="table-light sticky-top"><tr><th>3G</th><th>4G</th><th>5G</th></tr></thead><tbody>{% for r3, r4, r5 in kpi_rows %}<tr><td>{{ r3 or '-' }}</td><td>{{ r4 or '-' }}</td><td>{{ r5 or '-' }}</td></tr>{% endfor %}</tbody></table></div></div>
                 </div>
             </div>
        
        {% elif active_page == 'script' %}
             <div class="card border-0 shadow-sm"><div class="card-header bg-white fw-bold">Generate Script</div><div class="card-body">
                <ul class="nav nav-tabs mb-3" id="scriptTabs" role="tablist">
                    <li class="nav-item"><button class="nav-link active" data-bs-toggle="tab" data-bs-target="#tab3g900" type="button">3G 900</button></li>
                    <li class="nav-item"><button class="nav-link" data-bs-toggle="tab" data-bs-target="#tab4g" type="button">4G</button></li>
                    <li class="nav-item"><button class="nav-link" data-bs-toggle="tab" data-bs-target="#tab3g2100" type="button">3G 2100</button></li>
                </ul>
                <div class="tab-content">
                    <div class="tab-pane fade show active" id="tab3g900">
                        <form method="POST" action="/script"><input type="hidden" name="tech" value="3g900"><div class="table-responsive"><table class="table table-bordered" id="rruTable_3g900"><thead class="table-light"><tr><th>RRU Name</th><th>SRN</th><th>Slot</th><th>Port</th><th>RCN</th><th>SectorID</th><th>RX</th><th>TX</th><th>Action</th></tr></thead><tbody><tr><td><input type="text" name="rn[]" class="form-control" value="RRU1"></td><td><input type="number" name="srn[]" class="form-control" value="70"></td><td><input type="number" name="hsn[]" class="form-control" value="2"></td><td><input type="number" name="hpn[]" class="form-control" value="0"></td><td><input type="number" name="rcn[]" class="form-control" value="0"></td><td><input type="number" name="sectorid[]" class="form-control" value="0"></td><td><input type="number" name="rxnum[]" class="form-control" value="2"></td><td><input type="number" name="txnum[]" class="form-control" value="1"></td><td><button type="button" class="btn btn-danger btn-sm" onclick="this.closest('tr').remove()">X</button></td></tr></tbody></table></div><button type="button" class="btn btn-success mb-3" onclick="addRow('3g900')">+ Add RRU</button><br><button class="btn btn-primary shadow-sm">Generate Script</button></form>
                    </div>
                     <div class="tab-pane fade" id="tab4g">
                        <form method="POST" action="/script"><input type="hidden" name="tech" value="4g"><div class="table-responsive"><table class="table table-bordered" id="rruTable_4g"><thead class="table-light"><tr><th>RRU Name</th><th>SRN</th><th>Slot</th><th>Port</th><th>RCN</th><th>SectorID</th><th>RX</th><th>TX</th><th>Action</th></tr></thead><tbody><tr><td><input type="text" name="rn[]" class="form-control" value="RRU1"></td><td><input type="number" name="srn[]" class="form-control" value="60"></td><td><input type="number" name="hsn[]" class="form-control" value="3"></td><td><input type="number" name="hpn[]" class="form-control" value="0"></td><td><input type="number" name="rcn[]" class="form-control" value="0"></td><td><input type="number" name="sectorid[]" class="form-control" value="0"></td><td><input type="number" name="rxnum[]" class="form-control" value="4"></td><td><input type="number" name="txnum[]" class="form-control" value="4"></td><td><button type="button" class="btn btn-danger btn-sm" onclick="this.closest('tr').remove()">X</button></td></tr></tbody></table></div><button type="button" class="btn btn-success mb-3" onclick="addRow('4g')">+ Add RRU</button><br><button class="btn btn-primary shadow-sm">Generate Script</button></form>
                     </div>
                     <div class="tab-pane fade" id="tab3g2100">
                        <form method="POST" action="/script"><input type="hidden" name="tech" value="3g2100"><div class="table-responsive"><table class="table table-bordered" id="rruTable_3g2100"><thead class="table-light"><tr><th>RRU Name</th><th>SRN</th><th>Slot</th><th>Port</th><th>RCN</th><th>SectorID</th><th>RX</th><th>TX</th><th>Action</th></tr></thead><tbody><tr><td><input type="text" name="rn[]" class="form-control" value="RRU1"></td><td><input type="number" name="srn[]" class="form-control" value="80"></td><td><input type="number" name="hsn[]" class="form-control" value="3"></td><td><input type="number" name="hpn[]" class="form-control" value="0"></td><td><input type="number" name="rcn[]" class="form-control" value="0"></td><td><input type="number" name="sectorid[]" class="form-control" value="0"></td><td><input type="number" name="rxnum[]" class="form-control" value="2"></td><td><input type="number" name="txnum[]" class="form-control" value="1"></td><td><button type="button" class="btn btn-danger btn-sm" onclick="this.closest('tr').remove()">X</button></td></tr></tbody></table></div><button type="button" class="btn btn-success mb-3" onclick="addRow('3g2100')">+ Add RRU</button><br><button class="btn btn-primary shadow-sm">Generate Script</button></form>
                     </div>
                </div>
                {% if script_result %}
                <div class="mt-4"><h5 class="fw-bold text-primary">Result:</h5><textarea class="form-control font-monospace bg-light border-0" rows="12" readonly>{{ script_result }}</textarea></div>
                {% endif %}
             </div></div>

        {% endif %}
    </div>
</div>
{% endblock %}
"""

USER_MANAGEMENT_TEMPLATE = """
{% extends "base" %}
{% block content %}
<div class="row">
    <div class="col-md-4"><div class="card border-0 shadow-sm"><div class="card-header bg-white fw-bold">Add User</div><div class="card-body"><form method="POST" action="/users/add"><input name="username" class="form-control mb-2" placeholder="Username" required><input name="password" type="password" class="form-control mb-2" placeholder="Password" required><select name="role" class="form-select mb-3"><option value="user">User</option><option value="admin">Admin</option></select><button class="btn btn-success w-100 shadow-sm">Create</button></form></div></div></div>
    <div class="col-md-8"><div class="card border-0 shadow-sm"><div class="card-header bg-white fw-bold">Users</div><div class="table-responsive"><table class="table table-hover mb-0"><thead class="table-light"><tr><th>ID</th><th>User</th><th>Role</th><th>Action</th></tr></thead><tbody>{% for u in users %}<tr><td>{{ u.id }}</td><td class="fw-bold">{{ u.username }}</td><td><span class="badge bg-secondary">{{ u.role }}</span></td><td>{% if u.username!='admin' %}<a href="/users/delete/{{ u.id }}" class="btn btn-sm btn-danger shadow-sm">Del</a>{% endif %}</td></tr>{% endfor %}</tbody></table></div></div></div>
</div>
{% endblock %}
"""

PROFILE_TEMPLATE = """
{% extends "base" %}
{% block content %}
<div class="row justify-content-center"><div class="col-md-6"><div class="card border-0 shadow-sm"><div class="card-header bg-white fw-bold">Change Password</div><div class="card-body"><form method="POST" action="/change-password"><input type="password" name="current_password" class="form-control mb-3" placeholder="Current Password" required><input type="password" name="new_password" class="form-control mb-3" placeholder="New Password" required><button class="btn btn-primary w-100 shadow-sm">Save Changes</button></form></div></div></div></div>
{% endblock %}
"""

BACKUP_RESTORE_TEMPLATE = """
{% extends "base" %}
{% block content %}
<div class="container py-4">
    <div class="row g-4">
        <div class="col-md-6">
            <div class="card h-100 shadow-sm border-primary">
                <div class="card-header bg-primary text-white"><h5 class="mb-0"><i class="fa-solid fa-download me-2"></i>Backup Database</h5></div>
                <div class="card-body">
                    <form action="/backup" method="POST">
                        <div class="mb-3"><label class="form-label fw-bold">Select Tables to Backup:</label>
                        <div class="form-check"><input class="form-check-input" type="checkbox" id="selectAll" onclick="toggleCheckboxes(this)"><label class="form-check-label fw-bold" for="selectAll">Select All</label></div><hr class="my-2">
                        <div class="row">
                            <div class="col-md-6">
                                <div class="form-check"><input class="form-check-input" type="checkbox" name="tables" value="users.csv"> Users</div>
                                <div class="form-check"><input class="form-check-input" type="checkbox" name="tables" value="rf3g.csv"> RF 3G</div>
                                <div class="form-check"><input class="form-check-input" type="checkbox" name="tables" value="rf4g.csv"> RF 4G</div>
                                <div class="form-check"><input class="form-check-input" type="checkbox" name="tables" value="rf5g.csv"> RF 5G</div>
                            </div>
                            <div class="col-md-6">
                                <div class="form-check"><input class="form-check-input" type="checkbox" name="tables" value="poi4g.csv"> POI 4G</div>
                                <div class="form-check"><input class="form-check-input" type="checkbox" name="tables" value="poi5g.csv"> POI 5G</div>
                                <div class="form-check"><input class="form-check-input" type="checkbox" name="tables" value="kpi3g.csv"> KPI 3G</div>
                                <div class="form-check"><input class="form-check-input" type="checkbox" name="tables" value="kpi4g.csv"> KPI 4G</div>
                                <div class="form-check"><input class="form-check-input" type="checkbox" name="tables" value="kpi5g.csv"> KPI 5G</div>
                                <div class="form-check"><input class="form-check-input" type="checkbox" name="tables" value="qoe_4g.csv"> QoE 4G</div>
                                <div class="form-check"><input class="form-check-input" type="checkbox" name="tables" value="qos_4g.csv"> QoS 4G</div>
                            </div>
                        </div></div>
                        <button type="submit" class="btn btn-primary w-100 shadow-sm"><i class="fa-solid fa-file-zipper me-2"></i>Download Selected</button>
                    </form>
                </div>
            </div>
        </div>
        <div class="col-md-6">
            <div class="card h-100 shadow-sm border-warning">
                <div class="card-header bg-warning text-dark"><h5 class="mb-0"><i class="fa-solid fa-upload me-2"></i>Restore Database</h5></div>
                <div class="card-body">
                    <div class="alert alert-danger border-0 shadow-sm"><i class="fa-solid fa-triangle-exclamation me-2"></i><strong>WARNING:</strong> This will OVERWRITE existing data for the tables found in the zip file.</div>
                    <form action="/restore" method="POST" enctype="multipart/form-data">
                        <div class="mb-3"><label class="form-label fw-bold">Select Backup File (.zip)</label><input class="form-control border-0 shadow-sm" type="file" name="file" accept=".zip" required></div>
                        <button type="submit" class="btn btn-warning w-100 shadow-sm" onclick="return confirm('Are you sure you want to restore? This action cannot be undone.')"><i class="fa-solid fa-rotate-left me-2"></i>Restore Data</button>
                    </form>
                </div>
            </div>
        </div>
    </div>
</div>
{% endblock %}
"""

RF_FORM_TEMPLATE = """
{% extends "base" %}
{% block content %}
<div class="card border-0 shadow-sm"><div class="card-header bg-white fw-bold">{{ title }}</div><div class="card-body"><form method="POST"><div class="row">{% for col in columns %}<div class="col-md-4 mb-3"><label class="small text-muted fw-bold">{{ col }}</label><input type="text" name="{{ col }}" class="form-control" value="{{ obj[col] if obj else '' }}"></div>{% endfor %}</div><button type="submit" class="btn btn-primary shadow-sm">Save</button><a href="/rf?tech={{ tech }}" class="btn btn-secondary shadow-sm ms-2">Cancel</a></form></div></div>
{% endblock %}
"""

RF_DETAIL_TEMPLATE = """
{% extends "base" %}
{% block content %}
<div class="card border-0 shadow-sm"><div class="card-header bg-white fw-bold d-flex justify-content-between align-items-center"><span>Detail</span><a href="/rf?tech={{ tech }}" class="btn btn-secondary btn-sm shadow-sm">Quay lại</a></div><div class="card-body p-0 table-responsive"><table class="table table-bordered mb-0 table-striped">{% for k,v in obj.items() %}<tr><th class="w-25 text-end text-muted">{{ k }}</th><td class="fw-bold">{{ v }}</td></tr>{% endfor %}</table></div></div>
{% endblock %}
"""

app.jinja_loader = jinja2.DictLoader({
    'base': BASE_LAYOUT,
    'backup_restore': BACKUP_RESTORE_TEMPLATE
})

def render_page(tpl, **kwargs):
    if tpl == BACKUP_RESTORE_TEMPLATE: return render_template_string(tpl, **kwargs)
    return render_template_string(tpl, **kwargs)

# ==============================================================================
# 5. ROUTES
# ==============================================================================

def send_telegram_message(chat_id, text_content):
    if not TELEGRAM_BOT_TOKEN: return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    requests.post(url, json={"chat_id": chat_id, "text": text_content, "parse_mode": "HTML"})

def send_telegram_photo(chat_id, photo_url, caption=""):
    if not TELEGRAM_BOT_TOKEN: return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendPhoto"
    requests.post(url, json={"chat_id": chat_id, "photo": photo_url, "caption": caption, "parse_mode": "HTML"})

def process_bot_command(text):
    text = str(text).strip().upper()
    parts = text.split()
    if not parts: return "🤖 <b>Lỗi cú pháp!</b> Gõ <code>HELP</code> để xem hướng dẫn."
        
    cmd = parts[0]
    
    if cmd == 'HELP':
        return """🤖 <b>HƯỚNG DẪN SỬ DỤNG BOT TRA CỨU NETOPS</b>
Vui lòng gõ theo các cú pháp sau (không phân biệt hoa/thường):

👉 <code>DASHBOARD</code>: Tổng quan mạng 4G toàn hệ thống (7 ngày).
👉 <code>KPI [Mã_Cell]</code>: Tra cứu thông số KPI ngày mới nhất (VD: KPI THA001_1).
👉 <code>CHARTKPI [Mã_Cell]</code>: Tra cứu biểu đồ KPI 7 ngày gần nhất.
👉 <code>RF [Mã_Cell]</code>: Tra cứu tất cả thông số cấu hình trạm.
👉 <code>CTS [Mã_Cell]</code>: Tra cứu thông số QoE, QoS tuần mới nhất.
👉 <code>CHARTCTS [Mã_Cell]</code>: Tra cứu biểu đồ QoE, QoS 4 tuần mới nhất.

<i>*Lưu ý: Mặc định tra cứu mạng 4G. Có thể thêm 3G/5G vào giữa câu lệnh (VD: KPI 3G THA001).</i>"""
    
    with app.app_context():
        if cmd == 'DASHBOARD':
            records = db.session.query(
                KPI4G.thoi_gian,
                func.sum(KPI4G.traffic).label('traffic'),
                func.avg(KPI4G.user_dl_avg_thput).label('user_dl_avg_thput'),
                func.avg(KPI4G.res_blk_dl).label('res_blk_dl'),
                func.avg(KPI4G.cqi_4g).label('cqi_4g')
            ).group_by(KPI4G.thoi_gian).order_by(KPI4G.thoi_gian.desc()).limit(7).all()

            if not records:
                return "❌ Chưa có dữ liệu hệ thống 4G."

            records.reverse()
            labels = [r[0] for r in records if r[0]]

            def create_dash_url(label, data, color, title):
                cfg = {
                    "type": "line",
                    "data": {"labels": labels, "datasets": [{"label": label, "data": data, "borderColor": color, "backgroundColor": "transparent", "borderWidth": 3}]},
                    "options": {"title": {"display": True, "text": title, "fontSize": 16}, "elements": {"line": {"tension": 0.3}}}
                }
                return f"https://quickchart.io/chart?c={urllib.parse.quote(json.dumps(cfg))}&w=600&h=350&bkg=white"

            charts_to_send = []
            metrics = [
                ("Total Traffic (GB)", [round(r[1] or 0, 2) for r in records], "#0078d4", "Tổng Traffic 4G (7 Ngày)"),
                ("Avg Thput (Mbps)", [round(r[2] or 0, 2) for r in records], "#107c10", "Trung bình Tốc độ DL (7 Ngày)"),
                ("Avg PRB (%)", [round(r[3] or 0, 2) for r in records], "#ffaa44", "Trung bình Tải PRB (7 Ngày)"),
                ("Avg CQI", [round(r[4] or 0, 2) for r in records], "#00bcf2", "Trung bình CQI 4G (7 Ngày)")
            ]

            for label, data, color, title in metrics:
                charts_to_send.append({
                    "type": "photo",
                    "url": create_dash_url(label, data, color, title),
                    "caption": f"📈 <b>{title}</b> toàn mạng."
                })
            return charts_to_send

        if len(parts) < 2:
            return "🤖 <b>Lỗi cú pháp!</b> Vui lòng nhập đúng mẫu. (VD: <code>KPI THA001</code>)"

        target = parts[-1] 
        
        if cmd == 'CTS':
            qoe = QoE4G.query.filter(QoE4G.cell_name.ilike(f"%{target}%")).order_by(QoE4G.id.desc()).first()
            qos = QoS4G.query.filter(QoS4G.cell_name.ilike(f"%{target}%")).order_by(QoS4G.id.desc()).first()

            if not qoe and not qos:
                return f"❌ Không tìm thấy dữ liệu QoE/QoS cho Cell: <b>{target}</b>"

            msg = f"🌟 <b>THÔNG SỐ QoE / QoS - {target}</b>\n\n"
            if qoe:
                msg += f"📅 <b>{qoe.week_name}</b>\n- Điểm QoE: {qoe.qoe_score} ⭐\n- Tỷ lệ QoE: {qoe.qoe_percent} %\n\n"
            if qos:
                if not qoe or qoe.week_name != qos.week_name:
                    msg += f"📅 <b>{qos.week_name}</b>\n"
                msg += f"- Điểm QoS: {qos.qos_score} ⭐\n- Tỷ lệ QoS: {qos.qos_percent} %\n"
            return msg

        if cmd == 'CHARTCTS':
            qoe_records = QoE4G.query.filter(QoE4G.cell_name.ilike(f"%{target}%")).order_by(QoE4G.id.desc()).limit(4).all()
            qos_records = QoS4G.query.filter(QoS4G.cell_name.ilike(f"%{target}%")).order_by(QoS4G.id.desc()).limit(4).all()

            if not qoe_records and not qos_records:
                return f"❌ Không tìm thấy dữ liệu QoE/QoS cho Cell: <b>{target}</b>"

            all_weeks = sorted(list(set([r.week_name for r in qoe_records] + [r.week_name for r in qos_records])))
            all_weeks = all_weeks[-4:]

            def create_cts_url(label, data, color, title):
                cfg = {
                    "type": "line",
                    "data": {"labels": all_weeks, "datasets": [{"label": label, "data": data, "borderColor": color, "backgroundColor": "transparent", "borderWidth": 3}]},
                    "options": {"title": {"display": True, "text": title, "fontSize": 16}, "elements": {"line": {"tension": 0.3}}}
                }
                return f"https://quickchart.io/chart?c={urllib.parse.quote(json.dumps(cfg))}&w=600&h=350&bkg=white"

            charts_to_send = []
            c_name = (qoe_records[0].cell_name if qoe_records else qos_records[0].cell_name)

            if qoe_records:
                qmap = {r.week_name: r.qoe_score for r in qoe_records}
                pmap = {r.week_name: r.qoe_percent for r in qoe_records}
                charts_to_send.append({"type": "photo", "url": create_cts_url("Điểm QoE", [qmap.get(w, 0) for w in all_weeks], "#0078d4", f"Điểm QoE (4 Tuần) - {c_name}"), "caption": f"📈 Điểm QoE của {c_name}"})
                charts_to_send.append({"type": "photo", "url": create_cts_url("% QoE", [pmap.get(w, 0) for w in all_weeks], "#107c10", f"% QoE (4 Tuần) - {c_name}"), "caption": f"📈 Tỷ lệ % QoE của {c_name}"})

            if qos_records:
                qmap = {r.week_name: r.qos_score for r in qos_records}
                pmap = {r.week_name: r.qos_percent for r in qos_records}
                charts_to_send.append({"type": "photo", "url": create_cts_url("Điểm QoS", [qmap.get(w, 0) for w in all_weeks], "#ffaa44", f"Điểm QoS (4 Tuần) - {c_name}"), "caption": f"📈 Điểm QoS của {c_name}"})
                charts_to_send.append({"type": "photo", "url": create_cts_url("% QoS", [pmap.get(w, 0) for w in all_weeks], "#e3008c", f"% QoS (4 Tuần) - {c_name}"), "caption": f"📈 Tỷ lệ % QoS của {c_name}"})

            return charts_to_send

        tech = '4g'
        if len(parts) >= 3 and parts[1].lower() in ['3g', '4g', '5g']:
            tech = parts[1].lower()

        if cmd == 'KPI':
            Model = {'3g': KPI3G, '4g': KPI4G, '5g': KPI5G}.get(tech)
            if not Model: return "❌ Công nghệ không hợp lệ"
            record = Model.query.filter(Model.ten_cell.ilike(f"%{target}%")).order_by(Model.id.desc()).first()
            if record:
                if tech == '4g': return f"📊 <b>KPI 4G - {record.ten_cell}</b>\n📅 Ngày: {record.thoi_gian}\n- Traffic: {record.traffic} GB\n- Avg Thput: {record.user_dl_avg_thput} Mbps\n- PRB: {record.res_blk_dl}%\n- CQI: {record.cqi_4g}\n- Drop Rate: {record.service_drop_all}%"
                elif tech == '3g': return f"📊 <b>KPI 3G - {record.ten_cell}</b>\n📅 Ngày: {record.thoi_gian}\n- CS Traffic: {record.traffic} Erl\n- PS Traffic: {record.pstraffic} GB\n- CS Conges: {record.csconges}%\n- PS Conges: {record.psconges}%"
                else: return f"📊 <b>KPI 5G - {record.ten_cell}</b>\n📅 Ngày: {record.thoi_gian}\n- Traffic: {record.traffic} GB\n- Avg Thput: {record.user_dl_avg_throughput} Mbps\n- CQI 5G: {record.cqi_5g}"
            return f"❌ Không tìm thấy dữ liệu KPI cho Cell: <b>{target}</b>"
            
        elif cmd == 'RF':
            Model = {'3g': RF3G, '4g': RF4G, '5g': RF5G}.get(tech)
            if not Model: return "❌ Công nghệ không hợp lệ"
            record = Model.query.filter(Model.cell_code.ilike(f"%{target}%")).first()
            if record:
                if tech == '4g': return f"📡 <b>RF 4G - {record.cell_code}</b>\n📍 Trạm: {record.site_code}\n- Tọa độ: {record.latitude}, {record.longitude}\n- Azimuth: {record.azimuth}\n- Tilt: {record.total_tilt}\n- Tần số: {record.frequency}\n- ENodeB: {record.enodeb_id}\n- LCRID: {record.lcrid}"
                elif tech == '3g': return f"📡 <b>RF 3G - {record.cell_code}</b>\n📍 Trạm: {record.site_code}\n- Tọa độ: {record.latitude}, {record.longitude}\n- Azimuth: {record.azimuth}\n- Tần số: {record.frequency}\n- BSC_LAC: {record.bsc_lac}\n- CI: {record.ci}"
                elif tech == '5g': return f"📡 <b>RF 5G - {record.cell_code}</b>\n📍 Trạm: {record.site_code}\n- Tọa độ: {record.latitude}, {record.longitude}\n- Azimuth: {record.azimuth}\n- Tần số: {record.frequency}\n- GNodeB: {record.gnodeb_id}\n- LCRID: {record.lcrid}"
            return f"❌ Không tìm thấy cấu hình RF cho Cell: <b>{target}</b>"
            
        elif cmd in ['CHARTKPI', 'CHART', 'BIEUDO']:
            Model = {'3g': KPI3G, '4g': KPI4G, '5g': KPI5G}.get(tech)
            if not Model: return "❌ Công nghệ không hợp lệ"
            records = db.session.query(Model).filter(Model.ten_cell.ilike(f"%{target}%")).order_by(Model.id.desc()).limit(7).all()
            if not records: return f"❌ Không tìm thấy dữ liệu KPI cho Cell: <b>{target}</b>"
            
            records.reverse()
            labels = [r.thoi_gian for r in records if r.thoi_gian]
            charts_to_send = []
            
            def create_chart_url(label, data, color, title):
                chart_config = {
                    "type": "line",
                    "data": {"labels": labels, "datasets": [{"label": label, "data": data, "borderColor": color, "backgroundColor": "transparent", "borderWidth": 3}]},
                    "options": {"title": {"display": True, "text": title, "fontSize": 16}, "elements": {"line": {"tension": 0.3}}}
                }
                return f"https://quickchart.io/chart?c={urllib.parse.quote(json.dumps(chart_config))}&w=600&h=350&bkg=white"

            cell_name = records[0].ten_cell
            if tech == '4g':
                kpis = [("Traffic (GB)", [r.traffic or 0 for r in records], "#0078d4"), ("Avg Thput (Mbps)", [r.user_dl_avg_thput or 0 for r in records], "#107c10"), ("PRB DL (%)", [r.res_blk_dl or 0 for r in records], "#ffaa44"), ("CQI", [r.cqi_4g or 0 for r in records], "#00bcf2")]
            elif tech == '3g':
                kpis = [("CS Traffic (Erl)", [r.traffic or 0 for r in records], "#0078d4"), ("PS Traffic (GB)", [r.pstraffic or 0 for r in records], "#107c10"), ("CS Congestion (%)", [r.csconges or 0 for r in records], "#d13438"), ("PS Congestion (%)", [r.psconges or 0 for r in records], "#e3008c")]
            else:
                kpis = [("Traffic (GB)", [r.traffic or 0 for r in records], "#0078d4"), ("Avg Thput (Mbps)", [r.user_dl_avg_throughput or 0 for r in records], "#107c10"), ("CQI 5G", [r.cqi_5g or 0 for r in records], "#00bcf2")]
                
            for label, data, color in kpis:
                charts_to_send.append({"type": "photo", "url": create_chart_url(label, data, color, f"Biểu đồ {label} 7 Ngày - {cell_name}"), "caption": f"📈 <b>{label}</b> của {cell_name}"})
            return charts_to_send
            
    return "🤖 Cú pháp không được hỗ trợ. Gõ <code>HELP</code> để xem hướng dẫn."

@app.route('/telegram/webhook', methods=['POST'])
def telegram_webhook():
    data = request.json
    if data and 'message' in data:
        chat_id = data['message']['chat']['id']
        text = data['message'].get('text', '')
        if text:
            reply_data = process_bot_command(text)
            if isinstance(reply_data, list):
                for item in reply_data:
                    if item.get('type') == 'photo': send_telegram_photo(chat_id, item['url'], item.get('caption', ''))
            elif isinstance(reply_data, dict) and reply_data.get('type') == 'photo':
                send_telegram_photo(chat_id, reply_data['url'], reply_data.get('caption', ''))
            else:
                send_telegram_message(chat_id, str(reply_data))
    return jsonify({"status": "success"}), 200

@app.route('/telegram/set_webhook')
def set_telegram_webhook():
    if not TELEGRAM_BOT_TOKEN: return "Missing Bot Token", 400
    webhook_url = request.host_url.rstrip('/') + url_for('telegram_webhook')
    try:
        r = requests.get(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/setWebhook?url={webhook_url}")
        return jsonify(r.json())
    except Exception as e: return str(e), 500

@app.route('/login', methods=['GET', 'POST'])
def login():
    if current_user.is_authenticated: return redirect(url_for('index'))
    if request.method == 'POST':
        user = User.query.filter_by(username=request.form['username']).first()
        if user and user.check_password(request.form['password']):
            login_user(user)
            return redirect(url_for('index'))
        flash('Login failed', 'danger')
    return render_template_string(LOGIN_PAGE)

@app.route('/logout')
@login_required
def logout():
    logout_user()
    return redirect(url_for('login'))

@app.route('/')
@login_required
def index():
    dashboard_data = {'labels': [], 'traffic': [], 'thput': [], 'prb': [], 'cqi': []}
    try:
        records = db.session.query(KPI4G.thoi_gian, func.sum(KPI4G.traffic).label('traffic'), func.avg(KPI4G.user_dl_avg_thput).label('user_dl_avg_thput'), func.avg(KPI4G.res_blk_dl).label('res_blk_dl'), func.avg(KPI4G.cqi_4g).label('cqi_4g')).group_by(KPI4G.thoi_gian).all()
        if records:
            agg_data = {}
            for r in records:
                if not r[0]: continue
                agg_data[r[0]] = {'traffic_sum': r[1] or 0, 'thput_avg': r[2] or 0, 'prb_avg': r[3] or 0, 'cqi_avg': r[4] or 0}
            sorted_dates = sorted(agg_data.keys(), key=lambda x: datetime.strptime(x, '%d/%m/%Y'))
            dashboard_data['labels'] = sorted_dates
            for d in sorted_dates:
                dashboard_data['traffic'].append(round(agg_data[d]['traffic_sum'], 2))
                dashboard_data['thput'].append(round(agg_data[d]['thput_avg'], 2))
                dashboard_data['prb'].append(round(agg_data[d]['prb_avg'], 2))
                dashboard_data['cqi'].append(round(agg_data[d]['cqi_avg'], 2))
    except Exception as e: pass
    gc.collect()
    return render_page(CONTENT_TEMPLATE, title="Dashboard", active_page='dashboard', dashboard_data=dashboard_data)

@app.route('/import', methods=['GET', 'POST'])
@login_required
def import_data():
    if current_user.role != 'admin': return redirect(url_for('index'))
    if request.method == 'POST':
        files = request.files.getlist('file')
        itype = request.form.get('type')
        cfg = {'cell3g': Cell3G, 'config3g': Config3G, '3g': RF3G, '4g': RF4G, '5g': RF5G, 'kpi3g': KPI3G, 'kpi4g': KPI4G, 'kpi5g': KPI5G, 'poi4g': POI4G, 'poi5g': POI5G}
        Model = cfg.get(itype)
        if Model:
            valid_cols = [c.key for c in Model.__table__.columns if c.key not in ['id', 'extra_data']]
            for file in files:
                try:
                    df_raw = pd.read_csv(file, encoding='utf-8-sig', on_bad_lines='skip') if file.filename.endswith('.csv') else pd.read_excel(file)
                    
                    # 1. Kiểm tra xem dòng 0 (Mặc định của Pandas) có phải là header chưa
                    cols_str = " ".join([str(c).lower() for c in df_raw.columns])
                    kw = ['cell', 'site', 'trạm', 'uarfcn', 'hệ thống', 'quản lý', 'thiết bị', 'latitude', 'longitude']
                    header_found = any(k in cols_str for k in kw)
                    
                    # 2. Nếu dòng 0 chưa phải là Header, mới tiến hành quét 20 dòng đầu tiên
                    if not header_found:
                        header_idx = -1
                        for i, row in df_raw.head(20).iterrows():
                            row_vals = [str(v).lower() for v in row.values if pd.notna(v)]
                            if any(k in " ".join(row_vals) for k in kw):
                                header_idx = i
                                break
                        if header_idx != -1:
                            df_raw.columns = df_raw.iloc[header_idx]
                            df_raw = df_raw.iloc[header_idx + 1:].reset_index(drop=True)
                    
                    df_raw = df_raw.dropna(how='all')
                    original_columns = list(df_raw.columns)
                    df_raw.columns = [clean_header(c) for c in df_raw.columns]
                    header_mapping = dict(zip(df_raw.columns, original_columns))
                    
                    records = []
                    for row in df_raw.to_dict('records'):
                        clean_row, extra = {}, {}
                        for k, v in row.items():
                            if pd.isna(v) or str(v).strip() == '': continue
                            if k in valid_cols: 
                                clean_row[k] = v
                            else: 
                                extra[header_mapping.get(k, k)] = str(v)
                        
                        # Trích xuất mã định danh linh hoạt cho từng loại bảng
                        c_code = None
                        if itype == 'cell3g':
                            c_code = clean_row.get('ten_tren_he_thong') or clean_row.get('cell_code') or clean_row.get('cell_name')
                        elif itype == 'config3g':
                            c_code = clean_row.get('cell_name') or clean_row.get('ma_tram') or clean_row.get('cell_code')
                        else:
                            c_code = clean_row.get('cell_code') or clean_row.get('cell_name')
                        
                        # Fallback quét trong extra nếu chưa tìm thấy mã
                        if not c_code and extra:
                            for ex_k, ex_v in extra.items():
                                w_lower = str(ex_k).lower()
                                if any(word in w_lower for word in ['cell', 'site', 'trạm', 'node', 'hệ thống']):
                                    c_code = ex_v
                                    break
                        
                        if c_code and str(c_code).strip() not in ['', 'nan', 'None']:
                            c_code_clean = str(c_code).strip()
                            
                            # Gán lại mã định danh chuẩn theo Schema của Database
                            if itype == 'cell3g':
                                clean_row['cell_code'] = c_code_clean
                                clean_row['ten_tren_he_thong'] = c_code_clean
                            elif itype == 'config3g':
                                clean_row['cell_name'] = c_code_clean
                            else:
                                if 'cell_code' in valid_cols: clean_row['cell_code'] = c_code_clean
                                if 'cell_name' in valid_cols: clean_row['cell_name'] = c_code_clean
                                
                            if hasattr(Model, 'extra_data') and extra: 
                                clean_row['extra_data'] = json.dumps(extra, ensure_ascii=False)
                                
                            records.append(clean_row)
                    
                    if records:
                        db.session.bulk_insert_mappings(Model, records)
                        db.session.commit()
                        flash(f'Đã import thành công {len(records)} dòng vào {itype.upper()}.', 'success')
                    else:
                        found_cols = ", ".join([str(c) for c in original_columns[:10]])
                        flash(f'Lỗi file {file.filename}: Không tìm thấy cột định danh Cell/Trạm. Các cột tìm thấy: {found_cols}', 'warning')
                        
                except Exception as e: 
                    err_msg = str(e)
                    if 'Unknown column' in err_msg:
                        flash(f'CẤU TRÚC DB BỊ LỖI: Bạn chưa xóa cấu trúc DB cũ. Hãy vào tab "Reset Data" (màu đỏ) và bấm "Reset Toàn Bộ Dữ Liệu RF" trước khi Import nhé!', 'danger')
                    else:
                        flash(f'Lỗi file {file.filename}: {err_msg}', 'danger')
        return redirect(url_for('import_data'))
        
    d3 = [d[0] for d in db.session.query(KPI3G.thoi_gian).distinct().order_by(KPI3G.thoi_gian.desc()).all()]
    d4 = [d[0] for d in db.session.query(KPI4G.thoi_gian).distinct().order_by(KPI4G.thoi_gian.desc()).all()]
    d5 = [d[0] for d in db.session.query(KPI5G.thoi_gian).distinct().order_by(KPI5G.thoi_gian.desc()).all()]
    today = datetime.now()
    year, week_num, _ = today.isocalendar()
    start_of_week = today - timedelta(days=today.weekday())
    end_of_week = start_of_week + timedelta(days=6)
    default_week_name = f"Tuần {week_num:02d} ({start_of_week.strftime('%d/%m')}-{end_of_week.strftime('%d/%m')})"
    return render_page(CONTENT_TEMPLATE, title="Data Import", active_page='import', kpi_rows=list(zip_longest(d3, d4, d5)), default_week_name=default_week_name)

@app.route('/reset-data', methods=['POST'])
@login_required
def reset_data():
    if current_user.role != 'admin': return redirect(url_for('index'))
    target = request.form.get('target')
    try:
        if target == 'rf':
            # Xóa các bảng 3G cụ thể để tạo lại schema
            db.session.execute(text("DROP TABLE IF EXISTS cell_3g"))
            db.session.execute(text("DROP TABLE IF EXISTS config_3g"))
            db.session.execute(text("DROP TABLE IF EXISTS rf_3g"))
            db.session.execute(text("DROP TABLE IF EXISTS rf_4g"))
            db.session.execute(text("DROP TABLE IF EXISTS rf_5g"))
            db.session.commit()
            db.create_all()
            flash('Đã Reset và cập nhật cấu trúc bảng RF thành công!', 'success')
        elif target == 'poi':
            db.session.query(POI4G).delete(); db.session.query(POI5G).delete()
            db.session.commit(); flash('Đã reset dữ liệu POI!', 'success')
    except Exception as e: db.session.rollback(); flash(f'Lỗi: {e}', 'danger')
    return redirect(url_for('import_data'))

# --- Keep other routes (GIS, KPI, etc.) as they are ---

@app.route('/gis', methods=['GET', 'POST'])
@login_required
def gis(): return render_page(CONTENT_TEMPLATE, title="Bản đồ Trực quan (GIS)", active_page='gis')

if __name__ == '__main__':
    app.run(debug=True)
