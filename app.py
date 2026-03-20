import os
import pandas as pd
import json
import gc
import re
import zipfile
import random
import math
import requests
import urllib.parse
from io import BytesIO, StringIO
from datetime import datetime, timedelta
from flask import Flask, render_template, request, redirect, url_for, flash, send_file, Response, stream_with_context, jsonify
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

TELEGRAM_BOT_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN', '')

db = SQLAlchemy(app)
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login'

# ==============================================================================
# 2. UTILS & ROBUST HEADER MAPPING
# ==============================================================================

def remove_accents(input_str):
    if not isinstance(input_str, str): return str(input_str)
    s1, s0 = u'ÀÁÂÃÈÉÊÌÍÒÓÔÕÙÚÝàáâãèéêìíòóôõùúýĂăĐđĨíŨũƠơƯưẠạẢảẤấẦầẨẩẪẫẬậẮắẰằẲẳẴẵẶặẸẹẺẻẼẽẾếỀềỂểỄễỆệỈỉỊịỌọỎỏỐốỒồỔổỖỗỘộỚớỜờỞởỠỡỢợỤụỦủỨứỪừỬửỮữỰựỲỳỴịỶảỸỹ', u'AAAAEEEIIOOOOUUYaaaaeeeiioooouuyAaDdIiUuOoUuAaAaAaAaAaAaAaAaAaAaAaAaEeEeEeEeEeEeEeEeIiIiOoOoOoOoOoOoOoOoOoOoOoOoOoUuUuUuUuUuUuUuYyYyYaYy'
    s = ''
    for c in input_str: s += s0[s1.index(c)] if c in s1 else c
    return s

def clean_header(col_name):
    if not isinstance(col_name, str): return str(col_name)
    
    # Ép tiêu đề thành chuỗi dính liền, không dấu, chữ thường để chống lỗi khoảng trắng/xuống dòng
    c_clean = re.sub(r'[^a-z0-9]', '', remove_accents(col_name).lower())
    
    strong_map = {
        'stt': 'stt',
        'manode': 'ma_node',
        'sitecode': 'site_code',
        'macell': 'cell_code',
        'cellid': 'cell_code',
        'thietbi': 'thiet_bi',
        'tinhtp': 'tinh_tp',
        'donviquanly': 'don_vi_quan_ly',
        'macsht': 'ma_csht',
        'loaitram': 'loai_tram',
        'sitename': 'site_name',
        'tentram': 'site_name',
        'cellname': 'cell_name',
        'tencell': 'cell_name',
        'cellnamealias': 'cell_name_alias',
        'ci': 'ci',
        'lac': 'lac',
        'bsclac': 'bsc_lac',
        'rac': 'rac',
        'bangtan': 'bang_tan',
        'dlpsc': 'dl_psc',
        'psc': 'psc',
        'dluarfcn': 'dl_uarfcn',
        'uarfcn': 'dl_uarfcn',
        'cpichpower': 'cpich_power',
        'maxpower': 'max_power',
        'totalpower': 'total_power',
        'dcsupport': 'dc_support',
        'oamip': 'oam_ip',
        'mechanicaltilt': 'mechanical_tilt',
        'mechainicaltilt': 'mechanical_tilt',
        'electricaltilt': 'electrical_tilt',
        'totaltilt': 'total_tilt',
        'tilt': 'total_tilt',
        'antennatype': 'antenna_type',
        'antennagain': 'antenna_gain',
        'gain': 'antenna_gain',
        'antennahigh': 'antenna_high',
        'antenheight': 'anten_height',
        'celltype': 'cell_type',
        'noofcarrier': 'no_of_carrier',
        'specialcoverage': 'special_coverage',
        'trangthai': 'trang_thai',
        'note': 'ghi_chu',
        'ghichu': 'ghi_chu',
        'tenquanly': 'ten_quan_ly',
        'tennguoiquanly': 'nguoi_quan_ly',
        'sdtnguoiquanly': 'sdt_nguoi_quan_ly',
        'ngayhoatdong': 'ngay_hoat_dong',
        'startday': 'start_day',
        'hoancanhradoi': 'hoan_canh_ra_doi',
        'loaianten': 'loai_anten',
        'antennatenhangsx': 'hang_sx_anten',
        'hangsx': 'hang_sx',
        'antennadaitanhoatdong': 'anten_dai_tan',
        'antennadungchung': 'anten_dung_chung',
        'antennasoport': 'anten_so_port',
        'tenloaitram': 'ten_loai_tram',
        'diachi': 'dia_chi',
        'macshtcuatram': 'csht_site',
        'macshtcuacell': 'csht_cell',
        'tendonvi': 'ten_don_vi',
        'tentrenhethong': 'ten_tren_he_thong',
        'latitude': 'latitude',
        'lat': 'latitude',
        'longitude': 'longitude',
        'longtitude': 'longitude',
        'long': 'longitude',
        'azimuth': 'azimuth',
        'enodebid': 'enodeb_id',
        'gnodebid': 'gnodeb_id',
        'pci': 'pci',
        'tac': 'tac',
        'mimo': 'mimo',
        'mt': 'm_t',
        'et': 'e_t',
        'nrarfcn': 'nrarfcn',
        'lcrid': 'lcrid',
        'dongbo': 'dong_bo',
        'matram': 'ma_tram',
        'equipment': 'equipment',
        'frequency': 'frequency',
        'frenquency': 'frequency',
        'networktech': 'networktech',
        'ultrafficvolumegb': 'ul_traffic_volume_gb',
        'dltrafficvolumegb': 'dl_traffic_volume_gb',
        'totaldatatrafficvolumegb': 'traffic',
        'celluplinkaveragethroughput': 'cell_uplink_average_throughput',
        'celldownlinkaveragethroughput': 'cell_downlink_average_throughput',
        'auserdownlinkaveragethroughput': 'user_dl_avg_throughput',
        'cellavaibilityrate': 'cell_avaibility_rate',
        'sgnbadditionsuccessrate': 'sgnb_addition_success_rate',
        'sgnbabnormalreleaserate': 'sgnb_abnormal_release_rate',
        'cqi5g': 'cqi_5g',
        'cqi4g': 'cqi_4g',
        'poi': 'poi_name'
    }
    
    if c_clean in strong_map:
        return strong_map[c_clean]
        
    # Fallback cho các cột lạ: Chuyển khoảng trắng thành gạch dưới
    clean_fb = re.sub(r'[^a-z0-9]', '_', remove_accents(col_name).lower())
    clean_fb = re.sub(r'_+', '_', clean_fb).strip('_')
    return clean_fb

def generate_colors(n):
    base = ['#0078d4', '#107c10', '#d13438', '#ffaa44', '#00bcf2', '#5c2d91', '#e3008c', '#b4009e']
    if n <= len(base): return base[:n]
    return base + ["#"+''.join([random.choice('0123456789ABCDEF') for j in range(6)]) for i in range(n - len(base))]

# ==============================================================================
# 3. MODELS
# ==============================================================================

class User(UserMixin, db.Model):
    __tablename__='user'; id=db.Column(db.Integer, primary_key=True); username=db.Column(db.String(50), unique=True, nullable=False); password_hash=db.Column(db.String(255), nullable=False); role=db.Column(db.String(20), default='user'); created_at=db.Column(db.DateTime, default=datetime.utcnow)
    def set_password(self, password): self.password_hash = generate_password_hash(password)
    def check_password(self, password): return check_password_hash(self.password_hash, password)

class Cell3G(db.Model):
    __tablename__='cell_3g'; id=db.Column(db.Integer, primary_key=True); stt=db.Column(db.String(20)); ma_node=db.Column(db.String(50)); site_code=db.Column(db.String(50)); cell_code=db.Column(db.String(100), index=True); thiet_bi=db.Column(db.String(50)); tinh_tp=db.Column(db.String(100)); latitude=db.Column(db.Float); longitude=db.Column(db.Float); azimuth=db.Column(db.Integer); loai_anten=db.Column(db.String(100)); hang_sx_anten=db.Column(db.String(100)); anten_dai_tan=db.Column(db.String(100)); anten_dung_chung=db.Column(db.String(50)); anten_so_port=db.Column(db.String(50)); antenna_gain=db.Column(db.Float); antenna_high=db.Column(db.Float); mechanical_tilt=db.Column(db.Float); electrical_tilt=db.Column(db.Float); total_tilt=db.Column(db.Float); ten_loai_tram=db.Column(db.String(100)); dia_chi=db.Column(db.String(255)); csht_site=db.Column(db.String(100)); csht_cell=db.Column(db.String(100)); ten_don_vi=db.Column(db.String(100)); bang_tan=db.Column(db.String(50)); ten_quan_ly=db.Column(db.String(100)); nguoi_quan_ly=db.Column(db.String(100)); sdt_nguoi_quan_ly=db.Column(db.String(50)); ngay_hoat_dong=db.Column(db.String(50)); hoan_canh_ra_doi=db.Column(db.Text); trang_thai=db.Column(db.String(50)); ghi_chu=db.Column(db.Text); ten_tren_he_thong=db.Column(db.String(100)); extra_data=db.Column(db.Text)

class Config3G(db.Model):
    __tablename__='config_3g'; id=db.Column(db.Integer, primary_key=True); stt=db.Column(db.String(20)); ma_node=db.Column(db.String(50)); site_code=db.Column(db.String(50)); cell_code=db.Column(db.String(100), index=True); thiet_bi=db.Column(db.String(50)); tinh_tp=db.Column(db.String(100)); don_vi_quan_ly=db.Column(db.String(100)); ma_csht=db.Column(db.String(100)); loai_tram=db.Column(db.String(50)); site_name=db.Column(db.String(100)); cell_name=db.Column(db.String(100)); cell_name_alias=db.Column(db.String(100)); ci=db.Column(db.String(50)); lac=db.Column(db.String(50)); rac=db.Column(db.String(50)); bang_tan=db.Column(db.String(50)); dl_psc=db.Column(db.String(50)); dl_uarfcn=db.Column(db.String(50)); cpich_power=db.Column(db.Float); max_power=db.Column(db.Float); total_power=db.Column(db.Float); dc_support=db.Column(db.String(50)); oam_ip=db.Column(db.String(50)); longitude=db.Column(db.Float); latitude=db.Column(db.Float); azimuth=db.Column(db.Integer); mechanical_tilt=db.Column(db.Float); electrical_tilt=db.Column(db.Float); total_tilt=db.Column(db.Float); antenna_type=db.Column(db.String(100)); antenna_gain=db.Column(db.Float); antenna_high=db.Column(db.Float); cell_type=db.Column(db.String(50)); no_of_carrier=db.Column(db.String(50)); special_coverage=db.Column(db.String(100)); trang_thai=db.Column(db.String(50)); ghi_chu=db.Column(db.Text); extra_data=db.Column(db.Text)

class RF3G(db.Model):
    __tablename__='rf_3g'; id=db.Column(db.Integer, primary_key=True); stt=db.Column(db.String(20)); cell_code=db.Column(db.String(100), index=True); site_code=db.Column(db.String(50)); ma_node=db.Column(db.String(50)); cell_name=db.Column(db.String(100)); cell_name_alias=db.Column(db.String(100)); site_name=db.Column(db.String(100)); loai_tram=db.Column(db.String(50)); thiet_bi=db.Column(db.String(50)); tinh_tp=db.Column(db.String(100)); don_vi_quan_ly=db.Column(db.String(100)); ma_csht=db.Column(db.String(100)); csht_site=db.Column(db.String(100)); csht_cell=db.Column(db.String(100)); ten_don_vi=db.Column(db.String(100)); latitude=db.Column(db.Float); longitude=db.Column(db.Float); azimuth=db.Column(db.Integer); mechanical_tilt=db.Column(db.Float); electrical_tilt=db.Column(db.Float); total_tilt=db.Column(db.Float); antenna_type=db.Column(db.String(100)); loai_anten=db.Column(db.String(100)); hang_sx_anten=db.Column(db.String(100)); anten_dai_tan=db.Column(db.String(100)); anten_dung_chung=db.Column(db.String(50)); anten_so_port=db.Column(db.String(50)); antenna_gain=db.Column(db.Float); antenna_high=db.Column(db.Float); bang_tan=db.Column(db.String(50)); lac=db.Column(db.String(50)); ci=db.Column(db.String(50)); rac=db.Column(db.String(50)); dl_uarfcn=db.Column(db.String(50)); dl_psc=db.Column(db.String(50)); cpich_power=db.Column(db.Float); max_power=db.Column(db.Float); total_power=db.Column(db.Float); dc_support=db.Column(db.String(50)); oam_ip=db.Column(db.String(50)); cell_type=db.Column(db.String(50)); no_of_carrier=db.Column(db.String(50)); special_coverage=db.Column(db.String(100)); trang_thai=db.Column(db.String(50)); ten_quan_ly=db.Column(db.String(100)); nguoi_quan_ly=db.Column(db.String(100)); sdt_nguoi_quan_ly=db.Column(db.String(50)); ngay_hoat_dong=db.Column(db.String(50)); hoan_canh_ra_doi=db.Column(db.Text); dia_chi=db.Column(db.String(255)); ghi_chu=db.Column(db.Text); extra_data=db.Column(db.Text)

class RF4G(db.Model):
    __tablename__='rf_4g'; id=db.Column(db.Integer, primary_key=True); cell_code=db.Column(db.String(50), index=True); site_code=db.Column(db.String(50)); cell_name=db.Column(db.String(100)); csht_code=db.Column(db.String(50)); latitude=db.Column(db.Float); longitude=db.Column(db.Float); antena=db.Column(db.String(100)); azimuth=db.Column(db.Integer); total_tilt=db.Column(db.Float); equipment=db.Column(db.String(50)); frequency=db.Column(db.String(50)); dl_uarfcn=db.Column(db.String(50)); pci=db.Column(db.String(50)); tac=db.Column(db.String(50)); enodeb_id=db.Column(db.String(50)); lcrid=db.Column(db.String(50)); anten_height=db.Column(db.Float); m_t=db.Column(db.Float); e_t=db.Column(db.Float); mimo=db.Column(db.String(50)); hang_sx=db.Column(db.String(50)); swap=db.Column(db.String(50)); start_day=db.Column(db.String(50)); ghi_chu=db.Column(db.String(255))

class RF5G(db.Model):
    __tablename__='rf_5g'; id=db.Column(db.Integer, primary_key=True); cell_code=db.Column(db.String(50), index=True); site_code=db.Column(db.String(50)); site_name=db.Column(db.String(100)); csht_code=db.Column(db.String(50)); latitude=db.Column(db.Float); longitude=db.Column(db.Float); antena=db.Column(db.String(100)); azimuth=db.Column(db.Integer); total_tilt=db.Column(db.Float); equipment=db.Column(db.String(50)); frequency=db.Column(db.String(50)); nrarfcn=db.Column(db.String(50)); pci=db.Column(db.String(50)); tac=db.Column(db.String(50)); gnodeb_id=db.Column(db.String(50)); lcrid=db.Column(db.String(50)); anten_height=db.Column(db.Float); m_t=db.Column(db.Float); e_t=db.Column(db.Float); mimo=db.Column(db.String(50)); hang_sx=db.Column(db.String(50)); dong_bo=db.Column(db.String(50)); start_day=db.Column(db.String(50)); ghi_chu=db.Column(db.String(255))

class POI4G(db.Model): __tablename__='poi_4g'; id=db.Column(db.Integer, primary_key=True); cell_code=db.Column(db.String(50)); site_code=db.Column(db.String(50)); poi_name=db.Column(db.String(200), index=True)
class POI5G(db.Model): __tablename__='poi_5g'; id=db.Column(db.Integer, primary_key=True); cell_code=db.Column(db.String(50)); site_code=db.Column(db.String(50)); poi_name=db.Column(db.String(200), index=True)
class QoE4G(db.Model): __tablename__='qoe_4g'; id=db.Column(db.Integer, primary_key=True); cell_name=db.Column(db.String(100), index=True); week_name=db.Column(db.String(100)); qoe_score=db.Column(db.Float); qoe_percent=db.Column(db.Float); details=db.Column(db.Text)
class QoS4G(db.Model): __tablename__='qos_4g'; id=db.Column(db.Integer, primary_key=True); cell_name=db.Column(db.String(100), index=True); week_name=db.Column(db.String(100)); qos_score=db.Column(db.Float); qos_percent=db.Column(db.Float); details=db.Column(db.Text)
class ITSLog(db.Model): __tablename__='its_log'; id=db.Column(db.Integer, primary_key=True); timestamp=db.Column(db.String(50)); latitude=db.Column(db.Float); longitude=db.Column(db.Float); networktech=db.Column(db.String(20)); level=db.Column(db.Float); qual=db.Column(db.Float); cellid=db.Column(db.String(50))

class KPI3G(db.Model):
    __tablename__='kpi_3g'; id=db.Column(db.Integer, primary_key=True); ten_cell=db.Column(db.String(100), index=True); thoi_gian=db.Column(db.String(50)); traffic=db.Column(db.Float); pstraffic=db.Column(db.Float); cssr=db.Column(db.Float); dcr=db.Column(db.Float); ps_cssr=db.Column(db.Float); ps_dcr=db.Column(db.Float); hsdpa_throughput=db.Column(db.Float); hsupa_throughput=db.Column(db.Float); cs_so_att=db.Column(db.Float); ps_so_att=db.Column(db.Float); csconges=db.Column(db.Float); psconges=db.Column(db.Float); stt=db.Column(db.String(50)); nha_cung_cap=db.Column(db.String(50)); tinh=db.Column(db.String(50)); ten_rnc=db.Column(db.String(100)); ma_vnp=db.Column(db.String(50)); loai_ne=db.Column(db.String(50)); lac=db.Column(db.String(50)); ci=db.Column(db.String(50))

class KPI4G(db.Model):
    __tablename__='kpi_4g'; id=db.Column(db.Integer, primary_key=True); ten_cell=db.Column(db.String(100), index=True); thoi_gian=db.Column(db.String(50)); traffic=db.Column(db.Float); traffic_vol_dl=db.Column(db.Float); traffic_vol_ul=db.Column(db.Float); cell_dl_avg_thputs=db.Column(db.Float); cell_ul_avg_thput=db.Column(db.Float); user_dl_avg_thput=db.Column(db.Float); user_ul_avg_thput=db.Column(db.Float); erab_ssrate_all=db.Column(db.Float); service_drop_all=db.Column(db.Float); unvailable=db.Column(db.Float); res_blk_dl=db.Column(db.Float); cqi_4g=db.Column(db.Float); stt=db.Column(db.String(50)); nha_cung_cap=db.Column(db.String(50)); tinh=db.Column(db.String(50)); ten_rnc=db.Column(db.String(100)); ma_vnp=db.Column(db.String(50)); loai_ne=db.Column(db.String(50)); enodeb_id=db.Column(db.String(50)); cell_id=db.Column(db.String(50))

class KPI5G(db.Model):
    __tablename__='kpi_5g'; id=db.Column(db.Integer, primary_key=True); ten_cell=db.Column(db.String(100), index=True); thoi_gian=db.Column(db.String(50)); traffic=db.Column(db.Float); dl_traffic_volume_gb=db.Column(db.Float); ul_traffic_volume_gb=db.Column(db.Float); cell_downlink_average_throughput=db.Column(db.Float); cell_uplink_average_throughput=db.Column(db.Float); user_dl_avg_throughput=db.Column(db.Float); cqi_5g=db.Column(db.Float); cell_avaibility_rate=db.Column(db.Float); sgnb_addition_success_rate=db.Column(db.Float); sgnb_abnormal_release_rate=db.Column(db.Float); nha_cung_cap=db.Column(db.String(50)); tinh=db.Column(db.String(50)); ten_gnodeb=db.Column(db.String(100)); ma_vnp=db.Column(db.String(50)); loai_ne=db.Column(db.String(50)); gnodeb_id=db.Column(db.String(50)); cell_id=db.Column(db.String(50))

@login_manager.user_loader
def load_user(user_id): return db.session.get(User, int(user_id))

def init_database():
    with app.app_context():
        try:
            inspector = inspect(db.engine)
            if 'config_3g' in inspector.get_table_names():
                existing_columns = [col['name'] for col in inspector.get_columns('config_3g')]
                if 'don_vi_quan_ly' not in existing_columns or 'cell_code' not in existing_columns:
                    db.session.execute(text("DROP TABLE IF EXISTS cell_3g")); db.session.execute(text("DROP TABLE IF EXISTS config_3g")); db.session.execute(text("DROP TABLE IF EXISTS rf_3g"))
                    db.session.commit()
        except Exception as e: print("Auto-migration check failed:", e)

        db.create_all()
        if not User.query.filter_by(username='admin').first():
            u = User(username='admin', role='admin'); u.set_password('admin123')
            db.session.add(u); db.session.commit()
init_database()

# ==============================================================================
# 4. TELEGRAM & HELPERS
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

            if not records: return "❌ Chưa có dữ liệu hệ thống 4G."

            records.reverse()
            labels = [r[0] for r in records if r[0]]

            def create_dash_url(label, data, color, title):
                cfg = {"type": "line", "data": {"labels": labels, "datasets": [{"label": label, "data": data, "borderColor": color, "backgroundColor": "transparent", "borderWidth": 3}]}, "options": {"title": {"display": True, "text": title, "fontSize": 16}, "elements": {"line": {"tension": 0.3}}}}
                return f"https://quickchart.io/chart?c={urllib.parse.quote(json.dumps(cfg))}&w=600&h=350&bkg=white"

            charts_to_send = []
            metrics = [("Total Traffic (GB)", [round(r[1] or 0, 2) for r in records], "#0078d4", "Tổng Traffic 4G (7 Ngày)"), ("Avg Thput (Mbps)", [round(r[2] or 0, 2) for r in records], "#107c10", "Trung bình Tốc độ DL (7 Ngày)"), ("Avg PRB (%)", [round(r[3] or 0, 2) for r in records], "#ffaa44", "Trung bình Tải PRB (7 Ngày)"), ("Avg CQI", [round(r[4] or 0, 2) for r in records], "#00bcf2", "Trung bình CQI 4G (7 Ngày)")]
            for label, data, color, title in metrics: charts_to_send.append({"type": "photo", "url": create_dash_url(label, data, color, title), "caption": f"📈 <b>{title}</b> toàn mạng."})
            return charts_to_send

        if len(parts) < 2: return "🤖 <b>Lỗi cú pháp!</b> Vui lòng nhập đúng mẫu. (VD: <code>KPI THA001</code>)"

        target = parts[-1] 
        
        if cmd == 'CTS':
            qoe = QoE4G.query.filter(QoE4G.cell_name.ilike(f"%{target}%")).order_by(QoE4G.id.desc()).first()
            qos = QoS4G.query.filter(QoS4G.cell_name.ilike(f"%{target}%")).order_by(QoS4G.id.desc()).first()
            if not qoe and not qos: return f"❌ Không tìm thấy dữ liệu QoE/QoS cho Cell: <b>{target}</b>"
            msg = f"🌟 <b>THÔNG SỐ QoE / QoS - {target}</b>\n\n"
            if qoe: msg += f"📅 <b>{qoe.week_name}</b>\n- Điểm QoE: {qoe.qoe_score} ⭐\n- Tỷ lệ QoE: {qoe.qoe_percent} %\n\n"
            if qos:
                if not qoe or qoe.week_name != qos.week_name: msg += f"📅 <b>{qos.week_name}</b>\n"
                msg += f"- Điểm QoS: {qos.qos_score} ⭐\n- Tỷ lệ QoS: {qos.qos_percent} %\n"
            return msg

        if cmd == 'CHARTCTS':
            qoe_records = QoE4G.query.filter(QoE4G.cell_name.ilike(f"%{target}%")).order_by(QoE4G.id.desc()).limit(4).all()
            qos_records = QoS4G.query.filter(QoS4G.cell_name.ilike(f"%{target}%")).order_by(QoS4G.id.desc()).limit(4).all()
            if not qoe_records and not qos_records: return f"❌ Không tìm thấy dữ liệu QoE/QoS cho Cell: <b>{target}</b>"
            all_weeks = sorted(list(set([r.week_name for r in qoe_records] + [r.week_name for r in qos_records])))[-4:]

            def create_cts_url(label, data, color, title):
                cfg = {"type": "line", "data": {"labels": all_weeks, "datasets": [{"label": label, "data": data, "borderColor": color, "backgroundColor": "transparent", "borderWidth": 3}]}, "options": {"title": {"display": True, "text": title, "fontSize": 16}, "elements": {"line": {"tension": 0.3}}}}
                return f"https://quickchart.io/chart?c={urllib.parse.quote(json.dumps(cfg))}&w=600&h=350&bkg=white"

            charts_to_send = []
            c_name = (qoe_records[0].cell_name if qoe_records else qos_records[0].cell_name)
            if qoe_records:
                charts_to_send.append({"type": "photo", "url": create_cts_url("Điểm QoE", [{r.week_name: r.qoe_score for r in qoe_records}.get(w, 0) for w in all_weeks], "#0078d4", f"Điểm QoE (4 Tuần) - {c_name}"), "caption": f"📈 Điểm QoE của {c_name}"})
                charts_to_send.append({"type": "photo", "url": create_cts_url("% QoE", [{r.week_name: r.qoe_percent for r in qoe_records}.get(w, 0) for w in all_weeks], "#107c10", f"% QoE (4 Tuần) - {c_name}"), "caption": f"📈 Tỷ lệ % QoE của {c_name}"})
            if qos_records:
                charts_to_send.append({"type": "photo", "url": create_cts_url("Điểm QoS", [{r.week_name: r.qos_score for r in qos_records}.get(w, 0) for w in all_weeks], "#ffaa44", f"Điểm QoS (4 Tuần) - {c_name}"), "caption": f"📈 Điểm QoS của {c_name}"})
                charts_to_send.append({"type": "photo", "url": create_cts_url("% QoS", [{r.week_name: r.qos_percent for r in qos_records}.get(w, 0) for w in all_weeks], "#e3008c", f"% QoS (4 Tuần) - {c_name}"), "caption": f"📈 Tỷ lệ % QoS của {c_name}"})
            return charts_to_send

        tech = '4g'
        if len(parts) >= 3 and parts[1].lower() in ['3g', '4g', '5g']: tech = parts[1].lower()

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
                chart_config = {"type": "line", "data": {"labels": labels, "datasets": [{"label": label, "data": data, "borderColor": color, "backgroundColor": "transparent", "borderWidth": 3}]}, "options": {"title": {"display": True, "text": title, "fontSize": 16}, "elements": {"line": {"tension": 0.3}}}}
                return f"https://quickchart.io/chart?c={urllib.parse.quote(json.dumps(chart_config))}&w=600&h=350&bkg=white"

            cell_name = records[0].ten_cell
            if tech == '4g': kpis = [("Traffic (GB)", [r.traffic or 0 for r in records], "#0078d4"), ("Avg Thput (Mbps)", [r.user_dl_avg_thput or 0 for r in records], "#107c10"), ("PRB DL (%)", [r.res_blk_dl or 0 for r in records], "#ffaa44"), ("CQI", [r.cqi_4g or 0 for r in records], "#00bcf2")]
            elif tech == '3g': kpis = [("CS Traffic (Erl)", [r.traffic or 0 for r in records], "#0078d4"), ("PS Traffic (GB)", [r.pstraffic or 0 for r in records], "#107c10"), ("CS Congestion (%)", [r.csconges or 0 for r in records], "#d13438"), ("PS Congestion (%)", [r.psconges or 0 for r in records], "#e3008c")]
            else: kpis = [("Traffic (GB)", [r.traffic or 0 for r in records], "#0078d4"), ("Avg Thput (Mbps)", [r.user_dl_avg_throughput or 0 for r in records], "#107c10"), ("CQI 5G", [r.cqi_5g or 0 for r in records], "#00bcf2")]
                
            for label, data, color in kpis: charts_to_send.append({"type": "photo", "url": create_chart_url(label, data, color, f"Biểu đồ {label} 7 Ngày - {cell_name}"), "caption": f"📈 <b>{label}</b> của {cell_name}"})
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
            else: send_telegram_message(chat_id, str(reply_data))
    return jsonify({"status": "success"}), 200

@app.route('/telegram/set_webhook')
def set_telegram_webhook():
    if not TELEGRAM_BOT_TOKEN: return "Missing Bot Token", 400
    webhook_url = request.host_url.rstrip('/') + url_for('telegram_webhook')
    try:
        r = requests.get(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/setWebhook?url={webhook_url}")
        return jsonify(r.json())
    except Exception as e: return str(e), 500

# ==============================================================================
# 5. CORE WEB ROUTES
# ==============================================================================

@app.route('/login', methods=['GET', 'POST'])
def login():
    if current_user.is_authenticated: return redirect(url_for('index'))
    if request.method == 'POST':
        user = User.query.filter_by(username=request.form['username']).first()
        if user and user.check_password(request.form['password']):
            login_user(user)
            return redirect(url_for('index'))
        flash('Login failed', 'danger')
    return render_template('login.html')

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
    return render_template('content.html', title="Dashboard", active_page='dashboard', dashboard_data=dashboard_data)

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
                    if file.filename.endswith('.csv'):
                        df_raw = pd.read_csv(file, encoding='utf-8-sig', on_bad_lines='skip', header=None, dtype=str)
                    else:
                        df_raw = pd.read_excel(file, header=None, dtype=str)
                    
                    header_idx = 0
                    max_matches = 0
                    kw_clean = ['cell', 'site', 'tram', 'uarfcn', 'he thong', 'quan ly', 'thiet bi', 'lat', 'long', 'stt', 'node', 'bsc', 'rnc', 'azimuth', 'tilt', 'power', 'gain', 'dia chi', 'csht']
                    
                    if len(df_raw) > 0:
                        for i in range(min(20, len(df_raw))):
                            row_vals = [remove_accents(str(v)).lower() for v in df_raw.iloc[i].values if pd.notna(v)]
                            matches = sum(1 for k in kw_clean if any(k in val for val in row_vals))
                            if matches > max_matches:
                                max_matches = matches
                                header_idx = i
                    
                    if max_matches > 0:
                        raw_cols = [str(c).strip() for c in df_raw.iloc[header_idx].values]
                        seen = {}
                        for j, c in enumerate(raw_cols):
                            if c in seen:
                                seen[c] += 1
                                raw_cols[j] = f"{c}_{seen[c]}"
                            else:
                                seen[c] = 0
                        df_raw.columns = raw_cols
                        df_raw = df_raw.iloc[header_idx + 1:].reset_index(drop=True)
                    else:
                        df_raw.columns = [str(c) for c in df_raw.iloc[0].values]
                        df_raw = df_raw.iloc[1:].reset_index(drop=True)

                    df_raw = df_raw.dropna(how='all')
                    original_columns = list(df_raw.columns)
                    df_raw.columns = [clean_header(c) for c in df_raw.columns]
                    header_mapping = dict(zip(df_raw.columns, original_columns))
                    
                    records = []
                    inserted_count = 0
                    BATCH_SIZE = 500
                    
                    for index, row in df_raw.iterrows():
                        clean_row, extra = {}, {}
                        for k, v in row.items():
                            if pd.isna(v): continue
                            val_str = str(v).strip()
                            # Loại bỏ các ký tự rác phổ biến trong file Excel
                            if val_str.lower() in ['', '-', 'nan', 'none', 'n/a', 'null', '?']: continue
                            
                            if k in valid_cols:
                                col_type = str(Model.__table__.columns[k].type)
                                if 'FLOAT' in col_type or 'INTEGER' in col_type:
                                    try:
                                        # Hỗ trợ parse số thập phân dùng dấu phẩy tiếng Việt (vd: 17,5)
                                        v_clean = val_str.replace(',', '.')
                                        v_num = float(v_clean)
                                        if 'INTEGER' in col_type: v_num = int(math.floor(v_num))
                                        clean_row[k] = v_num
                                    except:
                                        # NẾU CỘT LÀ SỐ NHƯNG LÀ CHỮ RÁC KHÔNG CONVERT ĐƯỢC -> ĐỂ NULL
                                        clean_row[k] = None
                                else:
                                    # NẾU CỘT LÀ STRING -> GIỮ NGUYÊN VẸN CÓ DẤU (Địa chỉ, Tên, v.v...)
                                    clean_row[k] = val_str
                            else: 
                                extra[header_mapping.get(k, k)] = val_str
                        
                        c_code = clean_row.get('cell_code') or clean_row.get('cell_name') or clean_row.get('ten_tren_he_thong') or clean_row.get('ma_node') or clean_row.get('site_code')
                        
                        if not c_code and extra:
                            for ex_k, ex_v in extra.items():
                                w_lower = str(ex_k).lower()
                                if any(word in w_lower for word in ['cell', 'site', 'trạm', 'node', 'hệ thống']):
                                    c_code = ex_v
                                    break
                        
                        if c_code and str(c_code).strip().lower() not in ['', 'nan', 'none', 'null']:
                            c_code_clean = str(c_code).strip()
                            clean_row['cell_code'] = c_code_clean
                            
                            if hasattr(Model, 'extra_data') and extra: 
                                clean_row['extra_data'] = json.dumps(extra, ensure_ascii=False)
                                
                            records.append(clean_row)
                            
                        if len(records) >= BATCH_SIZE:
                            db.session.bulk_insert_mappings(Model, records)
                            db.session.commit()
                            inserted_count += len(records)
                            records = [] 
                            gc.collect()
                    
                    if records:
                        db.session.bulk_insert_mappings(Model, records)
                        db.session.commit()
                        inserted_count += len(records)
                        
                    if inserted_count > 0:
                        flash(f'Đã import thành công {inserted_count} dòng vào {itype.upper()}.', 'success')
                    else:
                        found_cols = ", ".join([str(c) for c in original_columns[:10]])
                        flash(f'Lỗi file {file.filename}: Không tìm thấy dữ liệu hợp lệ. Các cột tìm thấy: {found_cols}', 'warning')
                        
                except Exception as e: 
                    err_msg = str(e)
                    db.session.rollback()
                    if 'Unknown column' in err_msg:
                        flash('CẤU TRÚC DB BỊ LỖI: Bạn cần Reset Database cấu hình trước khi import!', 'danger')
                    else:
                        flash(f'Lỗi file {file.filename}: {err_msg}', 'danger')
        
        elif itype in ['qoe4g', 'qos4g']:
            week_name = request.form.get('week_name', 'Tuần')
            TargetModel = QoE4G if itype == 'qoe4g' else QoS4G
            for file in files:
                try:
                    df = pd.read_excel(file, header=None, dtype=str) if file.filename.endswith('.xlsx') else pd.read_csv(file, header=None, dtype=str)
                    header_row_idx, cell_col_idx = -1, -1
                    for i, row in df.iterrows():
                        for j, val in enumerate(row):
                            if str(val).lower().strip() in ['cell name', 'tên cell', 'cell_name']:
                                header_row_idx, cell_col_idx = i, j
                                break
                        if header_row_idx != -1: break
                        
                    if header_row_idx != -1 and cell_col_idx != -1:
                        headers = [" - ".join([str(df.iloc[i, j]).strip() for i in range(header_row_idx + 1) if str(df.iloc[i, j]).strip() not in ['nan', 'None', '']]) or f"Col_{j}" for j in range(len(df.columns))]
                        records = []
                        inserted_count = 0
                        BATCH_SIZE = 500
                        
                        for i in range(header_row_idx + 1, len(df)):
                            row_data = df.iloc[i]
                            c_name = str(row_data[cell_col_idx]).strip()
                            if not c_name or str(c_name).lower() in ['nan', 'none', 'null', ''] or len(str(c_name)) < 5 or str(c_name).isdigit(): continue
                            
                            try: val1 = float(row_data[cell_col_idx + 2])
                            except: val1 = 0.0
                            try: val2 = float(row_data[cell_col_idx + 3])
                            except: val2 = 0.0
                            
                            if math.isnan(val1): val1 = 0.0
                            if math.isnan(val2): val2 = 0.0
                                
                            percent, score = max(val1, val2), min(val1, val2)
                            details_dict = {headers[j]: str(row_data[j]).strip() for j in range(len(headers)) if pd.notna(row_data[j]) and str(row_data[j]).strip() not in ['nan', 'None', '']}
                            details_json = json.dumps(details_dict, ensure_ascii=False)
                            
                            records.append({'cell_name': c_name, 'week_name': week_name, 'qoe_score' if itype == 'qoe4g' else 'qos_score': score, 'qoe_percent' if itype == 'qoe4g' else 'qos_percent': percent, 'details': details_json})
                            
                            if len(records) >= BATCH_SIZE:
                                db.session.bulk_insert_mappings(TargetModel, records)
                                db.session.commit()
                                inserted_count += len(records)
                                records = []
                                gc.collect()
                                
                        if records:
                            db.session.bulk_insert_mappings(TargetModel, records)
                            db.session.commit()
                            inserted_count += len(records)
                            
                        flash(f'Import thành công {inserted_count} dòng.', 'success')
                except Exception as e: flash(f'Lỗi: {e}', 'danger')

        return redirect(url_for('import_data'))
    
    d3 = [d[0] for d in db.session.query(KPI3G.thoi_gian).distinct().order_by(KPI3G.thoi_gian.desc()).all()]
    d4 = [d[0] for d in db.session.query(KPI4G.thoi_gian).distinct().order_by(KPI4G.thoi_gian.desc()).all()]
    d5 = [d[0] for d in db.session.query(KPI5G.thoi_gian).distinct().order_by(KPI5G.thoi_gian.desc()).all()]
    today = datetime.now()
    year, week_num, _ = today.isocalendar()
    start_of_week = today - timedelta(days=today.weekday())
    end_of_week = start_of_week + timedelta(days=6)
    default_week_name = f"Tuần {week_num:02d} ({start_of_week.strftime('%d/%m')}-{end_of_week.strftime('%d/%m')})"
    return render_template('content.html', title="Data Import", active_page='import', kpi_rows=list(zip_longest(d3, d4, d5)), default_week_name=default_week_name)

@app.route('/sync-rf3g', methods=['POST'])
@login_required
def sync_rf3g():
    if current_user.role != 'admin': return redirect(url_for('index'))
    try:
        db.session.query(RF3G).delete()
        cells = {str(c.cell_code).strip().upper(): c for c in Cell3G.query.all() if c.cell_code}
        configs = {str(c.cell_code).strip().upper(): c for c in Config3G.query.all() if c.cell_code}
        
        all_codes = set(cells.keys()) | set(configs.keys())
        
        rf3g_records = []
        inserted_count = 0
        BATCH_SIZE = 500
        
        for code in all_codes:
            c = cells.get(code)
            cfg = configs.get(code)
            
            merged_extra = {}
            if c and c.extra_data:
                try: merged_extra.update(json.loads(c.extra_data))
                except: pass
            if cfg and cfg.extra_data:
                try: merged_extra.update(json.loads(cfg.extra_data))
                except: pass
            
            record = RF3G(
                cell_code=code,
                site_code=getattr(c, 'site_code', getattr(cfg, 'site_code', None)),
                ma_node=getattr(c, 'ma_node', getattr(cfg, 'ma_node', None)),
                cell_name=getattr(cfg, 'cell_name', getattr(c, 'ten_tren_he_thong', None)),
                cell_name_alias=getattr(cfg, 'cell_name_alias', None),
                site_name=getattr(cfg, 'site_name', getattr(c, 'ten_loai_tram', None)),
                loai_tram=getattr(c, 'ten_loai_tram', getattr(cfg, 'loai_tram', None)),
                thiet_bi=getattr(c, 'thiet_bi', getattr(cfg, 'thiet_bi', None)),
                tinh_tp=getattr(c, 'tinh_tp', getattr(cfg, 'tinh_tp', None)),
                don_vi_quan_ly=getattr(cfg, 'don_vi_quan_ly', getattr(c, 'ten_don_vi', None)),
                ma_csht=getattr(cfg, 'ma_csht', None),
                csht_site=getattr(c, 'csht_site', None),
                csht_cell=getattr(c, 'csht_cell', None),
                ten_don_vi=getattr(c, 'ten_don_vi', getattr(cfg, 'don_vi_quan_ly', None)),
                latitude=getattr(c, 'latitude', getattr(cfg, 'latitude', None)),
                longitude=getattr(c, 'longitude', getattr(cfg, 'longitude', None)),
                azimuth=getattr(c, 'azimuth', getattr(cfg, 'azimuth', None)),
                mechanical_tilt=getattr(c, 'mechanical_tilt', getattr(cfg, 'mechanical_tilt', None)),
                electrical_tilt=getattr(c, 'electrical_tilt', getattr(cfg, 'electrical_tilt', None)),
                total_tilt=getattr(c, 'total_tilt', getattr(cfg, 'total_tilt', None)),
                antenna_type=getattr(cfg, 'antenna_type', getattr(c, 'loai_anten', None)),
                loai_anten=getattr(c, 'loai_anten', None),
                hang_sx_anten=getattr(c, 'hang_sx_anten', None),
                anten_dai_tan=getattr(c, 'anten_dai_tan', None),
                anten_dung_chung=getattr(c, 'anten_dung_chung', None),
                anten_so_port=getattr(c, 'anten_so_port', None),
                antenna_gain=getattr(c, 'antenna_gain', getattr(cfg, 'antenna_gain', None)),
                antenna_high=getattr(c, 'antenna_high', getattr(cfg, 'antenna_high', None)),
                bang_tan=getattr(c, 'bang_tan', getattr(cfg, 'bang_tan', None)),
                lac=getattr(cfg, 'lac', getattr(c, 'lac', None)),
                ci=getattr(cfg, 'ci', getattr(c, 'ci', None)),
                rac=getattr(cfg, 'rac', None),
                dl_uarfcn=getattr(cfg, 'dl_uarfcn', None),
                dl_psc=getattr(c, 'dl_psc', getattr(cfg, 'dl_psc', None)),
                cpich_power=getattr(cfg, 'cpich_power', getattr(c, 'cpich_power', None)),
                max_power=getattr(cfg, 'max_power', None),
                total_power=getattr(cfg, 'total_power', getattr(c, 'total_power', None)),
                dc_support=getattr(cfg, 'dc_support', None),
                oam_ip=getattr(cfg, 'oam_ip', None),
                cell_type=getattr(cfg, 'cell_type', None),
                no_of_carrier=getattr(cfg, 'no_of_carrier', None),
                special_coverage=getattr(cfg, 'special_coverage', None),
                trang_thai=getattr(c, 'trang_thai', getattr(cfg, 'trang_thai', None)),
                ten_quan_ly=getattr(c, 'ten_quan_ly', getattr(c, 'nguoi_quan_ly', None)),
                sdt_nguoi_quan_ly=getattr(c, 'sdt_nguoi_quan_ly', None),
                ngay_hoat_dong=getattr(c, 'ngay_hoat_dong', None),
                hoan_canh_ra_doi=getattr(c, 'hoan_canh_ra_doi', None),
                dia_chi=getattr(c, 'dia_chi', None),
                extra_data=json.dumps(merged_extra, ensure_ascii=False) if merged_extra else None
            )
            rf3g_records.append(record)
            
            if len(rf3g_records) >= BATCH_SIZE:
                db.session.bulk_save_objects(rf3g_records)
                db.session.commit()
                inserted_count += len(rf3g_records)
                rf3g_records = []
                gc.collect()
            
        if rf3g_records:
            db.session.bulk_save_objects(rf3g_records)
            db.session.commit()
            inserted_count += len(rf3g_records)
            
        if inserted_count > 0:
            flash(f'Đã ghép nối và đồng bộ {inserted_count} trạm 3G thành công!', 'success')
        else:
            flash('Không có dữ liệu 3G để đồng bộ. Vui lòng kiểm tra lại.', 'warning')
    except Exception as e:
        db.session.rollback()
        flash(f'Lỗi đồng bộ: {str(e)}', 'danger')
    return redirect(url_for('import_data'))

@app.route('/reset-data', methods=['POST'])
@login_required
def reset_data():
    if current_user.role != 'admin': return redirect(url_for('index'))
    target = request.form.get('target')
    try:
        if target == 'rf':
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

@app.route('/gis', methods=['GET', 'POST'])
@login_required
def gis():
    action_type = request.form.get('action', 'search') if request.method == 'POST' else 'search'
    tech = request.form.get('tech', '4g') if request.method == 'POST' else request.args.get('tech', '4g')
    site_code_input = request.form.get('site_code', '').strip() if request.method == 'POST' else request.args.get('site_code', '').strip()
    cell_name_input = request.form.get('cell_name', '').strip() if request.method == 'POST' else request.args.get('cell_name', '').strip()
    
    show_its = False
    its_data = []
    matched_sites = set()
    gis_data = []

    def clean_val(v):
        if v is None: return None
        s = str(v).strip()
        if s == '-' or s == '' or s.lower() in ['nan', 'null', 'none']: return None
        try:
            f = float(s)
            if f.is_integer(): return str(int(f))
            return str(f)
        except ValueError: return s.upper()

    def safe_float(val, default=0.0):
        if val is None: return default
        s = str(val).strip()
        if not s or s == '-': return default
        try: return float(s)
        except ValueError: return default

    Model = {'3g': RF3G, '4g': RF4G, '5g': RF5G}.get(tech)
    db_mapping = {}
    
    if Model and action_type == 'show_log':
        if tech == '4g' and hasattr(Model, 'enodeb_id') and hasattr(Model, 'lcrid'):
            res = db.session.query(Model.site_code, Model.enodeb_id, Model.lcrid).all()
            for sc, en, lc in res:
                c_en, c_lc = clean_val(en), clean_val(lc)
                if sc and c_en and c_lc: db_mapping[f"{c_en}_{c_lc}"] = sc
        elif tech == '3g' and hasattr(Model, 'ci'):
            res = db.session.query(Model.site_code, Model.ci).all()
            for sc, ci in res:
                c_ci = clean_val(ci)
                if sc and c_ci: db_mapping[c_ci] = sc
        elif tech == '5g' and hasattr(Model, 'gnodeb_id') and hasattr(Model, 'lcrid'):
            res = db.session.query(Model.site_code, Model.gnodeb_id, Model.lcrid).all()
            for sc, gn, lc in res:
                c_gn, c_lc = clean_val(gn), clean_val(lc)
                if sc and c_gn and c_lc: db_mapping[f"{c_gn}_{c_lc}"] = sc
    
    if request.method == 'POST' and 'its_file' in request.files:
        files = request.files.getlist('its_file')
        for file in files:
            if file and file.filename:
                show_its = True
                try:
                    file_bytes = file.read()
                    if not file_bytes:
                        continue
                    
                    content = file_bytes.decode('utf-8-sig', errors='ignore')
                    lines = content.splitlines()
                    if len(lines) > 1:
                        header_line = lines[0]
                        sep = '|' if '|' in header_line else (',' if ',' in header_line else '\t')
                        headers = [h.strip().lower() for h in header_line.split(sep)]
                        
                        try:
                            lat_idx = next(i for i, h in enumerate(headers) if h in ['latitude', 'lat'])
                            lon_idx = next(i for i, h in enumerate(headers) if h in ['longitude', 'lon', 'long'])
                        except StopIteration:
                            lat_idx, lon_idx = -1, -1
                            
                        node_idx = next((i for i, h in enumerate(headers) if h in ['node', 'enodebid', 'enodeb_id']), -1)
                        cell_idx = next((i for i, h in enumerate(headers) if h in ['cellid', 'ci', 'cell_id']), -1)
                        level_idx = next((i for i, h in enumerate(headers) if h in ['level', 'rsrp', 'rscp', 'rxlev']), -1)
                        tech_idx = next((i for i, h in enumerate(headers) if h in ['networktech', 'tech', 'network_tech']), -1)
                        qual_idx = next((i for i, h in enumerate(headers) if h in ['qual', 'ecno', 'sinr', 'snr', 'rsrq']), -1)

                        if lat_idx != -1 and lon_idx != -1:
                            data_lines = lines[1:]
                            for line in data_lines:
                                if not line.strip(): continue
                                parts = line.split(sep)
                                if len(parts) <= max(lat_idx, lon_idx): continue
                                
                                try:
                                    lat_str, lon_str = parts[lat_idx].strip(), parts[lon_idx].strip()
                                    if not lat_str or lat_str == '-' or not lon_str or lon_str == '-': continue
                                    lat, lon = float(lat_str), float(lon_str)
                                    
                                    n = clean_val(parts[node_idx]) if node_idx >= 0 and len(parts) > node_idx else None
                                    c = clean_val(parts[cell_idx]) if cell_idx >= 0 and len(parts) > cell_idx else None
                                    
                                    if action_type == 'show_log':
                                        if tech == '4g' and n and c:
                                            if f"{n}_{c}" in db_mapping: matched_sites.add(db_mapping[f"{n}_{c}"])
                                        elif tech == '3g' and c:
                                            if c in db_mapping: matched_sites.add(db_mapping[c])
                                        elif tech == '5g' and n and c:
                                            if f"{n}_{c}" in db_mapping: matched_sites.add(db_mapping[f"{n}_{c}"])
                                                
                                    lvl = safe_float(parts[level_idx] if level_idx >= 0 and len(parts) > level_idx else '')
                                    qual_str = parts[qual_idx].strip() if qual_idx >= 0 and len(parts) > qual_idx else ''
                                    tech_str = parts[tech_idx].strip().upper() if tech_idx >= 0 and len(parts) > tech_idx else tech.upper()
                                    
                                    its_data.append({'lat': lat, 'lon': lon, 'level': lvl, 'qual': qual_str, 'tech': tech_str, 'cellid': c or '', 'node': n or ''})
                                except ValueError: pass
                except Exception as e: flash(f'Lỗi xử lý file {file.filename}: {e}', 'danger')
        
        if len(its_data) > 20000:
            its_data = random.sample(its_data, 20000)
            flash(f'Đã giới hạn hiển thị ngẫu nhiên 20,000 điểm đo từ tổng số để chống treo trình duyệt.', 'warning')
            
    if Model:
        if action_type == 'show_log' and show_its:
            query = db.session.query(Model)
            if matched_sites: query = query.filter(Model.site_code.in_(list(matched_sites)[:900]))
            else: query = query.filter(text("1=0"))
            records = query.all()
        else:
            records_dict = {}
            target_lat, target_lon = None, None
            found_search = False
            
            if site_code_input or cell_name_input:
                search_q = db.session.query(Model)
                if site_code_input: search_q = search_q.filter(Model.site_code.ilike(f"%{site_code_input}%"))
                if cell_name_input:
                    filters = [Model.cell_code.ilike(f"%{cell_name_input}%")]
                    if hasattr(Model, 'cell_name'): filters.append(Model.cell_name.ilike(f"%{cell_name_input}%"))
                    search_q = search_q.filter(or_(*filters))
                
                for r in search_q.limit(50).all():
                    records_dict[r.id] = r
                    if not found_search and r.latitude and r.longitude:
                        try:
                            target_lat, target_lon = float(r.latitude), float(r.longitude)
                            found_search = True
                        except: pass
            
            if found_search and target_lat and target_lon:
                lat_min, lat_max = target_lat - 0.15, target_lat + 0.15
                lon_min, lon_max = target_lon - 0.15, target_lon + 0.15
                nearby = db.session.query(Model).filter(
                    Model.latitude >= lat_min, Model.latitude <= lat_max,
                    Model.longitude >= lon_min, Model.longitude <= lon_max
                ).limit(2000).all()
                for r in nearby: records_dict[r.id] = r
            else:
                for r in db.session.query(Model).limit(3000).all(): records_dict[r.id] = r
                
            records = list(records_dict.values())

        cols = [c.key for c in Model.__table__.columns if c.key not in ['id', 'extra_data']]
        for r in records:
            try:
                lat, lon = float(r.latitude), float(r.longitude)
                azi = int(r.azimuth) if getattr(r, 'azimuth', None) is not None else 0
                if 8 <= lat <= 24 and 102 <= lon <= 110:
                    gis_data.append({'cell_name': getattr(r, 'cell_name', getattr(r, 'site_name', str(r.cell_code))), 'site_code': r.site_code, 'lat': lat, 'lon': lon, 'azi': azi, 'tech': tech, 'info': {c: getattr(r, c) or '' for c in cols}})
            except: pass
    gc.collect()
    return render_template('content.html', title="Bản đồ Trực quan (GIS)", active_page='gis', selected_tech=tech, site_code_input=site_code_input, cell_name_input=cell_name_input, gis_data=gis_data, its_data=its_data, show_its=show_its, action_type=action_type)

@app.route('/kpi')
@login_required
def kpi():
    selected_tech = request.args.get('tech', '4g')
    cell_name_input = request.args.get('cell_name', '').strip()
    poi_input = request.args.get('poi_name', '').strip()
    charts = {} 

    colors = generate_colors(20)
    target_cells = []
    KPI_Model = {'3g': KPI3G, '4g': KPI4G, '5g': KPI5G}.get(selected_tech)
    RF_Model = {'3g': RF3G, '4g': RF4G, '5g': RF5G}.get(selected_tech)

    if poi_input:
        POI_Model = {'4g': POI4G, '5g': POI5G}.get(selected_tech)
        if POI_Model: target_cells = [r.cell_code for r in POI_Model.query.filter(POI_Model.poi_name == poi_input).all()]
    elif cell_name_input:
        if RF_Model:
            matched_rf = RF_Model.query.filter(or_(RF_Model.site_code.ilike(f"%{cell_name_input}%"), RF_Model.cell_code.ilike(f"%{cell_name_input}%"))).all()
            if matched_rf: target_cells.extend([r.cell_code for r in matched_rf])
        if KPI_Model:
            matched_kpi = KPI_Model.query.filter(KPI_Model.ten_cell.ilike(f"%{cell_name_input}%")).with_entities(KPI_Model.ten_cell).distinct().all()
            if matched_kpi: target_cells.extend([r[0] for r in matched_kpi])
        if not target_cells: target_cells = [c.strip() for c in re.split(r'[,\s;]+', cell_name_input) if c.strip()]
            
    if target_cells:
        unique_cells = []
        seen = set()
        for c in target_cells:
            if not c: continue
            c_clean = str(c).strip().upper()
            if c_clean not in seen:
                seen.add(c_clean)
                unique_cells.append(str(c).strip())
        target_cells = unique_cells

    if target_cells and KPI_Model:
        data = KPI_Model.query.filter(KPI_Model.ten_cell.in_(target_cells)).all()
        try: data.sort(key=lambda x: datetime.strptime(x.thoi_gian, '%d/%m/%Y'))
        except: pass 

        if data:
            all_labels = sorted(list(set([x.thoi_gian for x in data])), key=lambda d: datetime.strptime(d, '%d/%m/%Y'))
            data_by_cell = defaultdict(list)
            for x in data: data_by_cell[str(x.ten_cell).strip().upper()].append(x)

            metrics_config = {
                '3g': [{'key': 'pstraffic', 'label': 'PSTRAFFIC (GB)'}, {'key': 'traffic', 'label': 'TRAFFIC (Erl)'}, {'key': 'psconges', 'label': 'PS CONGESTION (%)'}, {'key': 'csconges', 'label': 'CS CONGESTION (%)'}],
                '4g': [{'key': 'traffic', 'label': 'TOTAL TRAFFIC (GB)'}, {'key': 'user_dl_avg_thput', 'label': 'USER DL AVG THPUT (Mbps)'}, {'key': 'res_blk_dl', 'label': 'RES BLOCK DL (%)'}, {'key': 'cqi_4g', 'label': 'CQI 4G'}],
                '5g': [{'key': 'traffic', 'label': 'TOTAL TRAFFIC (GB)'}, {'key': 'user_dl_avg_throughput', 'label': 'USER DL AVG THPUT (Mbps)'}, {'key': 'cqi_5g', 'label': 'CQI 5G'}]
            }
            
            current_metrics = metrics_config.get(selected_tech, [])
            for metric in current_metrics:
                datasets = []
                for i, cell_code in enumerate(target_cells):
                    cell_data = data_by_cell.get(cell_code.upper(), [])
                    data_map = {item.thoi_gian: (getattr(item, metric['key'], 0) or 0) for item in cell_data}
                    datasets.append({'label': cell_code, 'data': [data_map.get(lbl, None) for lbl in all_labels], 'borderColor': colors[i % len(colors)], 'fill': False, 'spanGaps': True})
                charts[f"chart_{metric['key']}"] = {'title': metric['label'], 'labels': all_labels, 'datasets': datasets}

    poi_list = []
    with app.app_context():
        try:
            p4 = [r[0] for r in db.session.query(POI4G.poi_name).distinct()]
            p5 = [r[0] for r in db.session.query(POI5G.poi_name).distinct()]
            poi_list = sorted(list(set(p4 + p5)))
        except: pass

    gc.collect()
    return render_template('content.html', title="Báo cáo KPI", active_page='kpi', selected_tech=selected_tech, cell_name_input=cell_name_input, selected_poi=poi_input, poi_list=poi_list, charts=charts)

@app.route('/qoe-qos')
@login_required
def qoe_qos():
    cell_name_input = request.args.get('cell_name', '').strip()
    charts = {}
    has_data = False
    qoe_details, qos_details, qoe_headers, qos_headers = [], [], [], []
    
    if cell_name_input:
        qoe_records = QoE4G.query.filter(QoE4G.cell_name.ilike(f"%{cell_name_input}%")).order_by(QoE4G.id.asc()).all()
        qos_records = QoS4G.query.filter(QoS4G.cell_name.ilike(f"%{cell_name_input}%")).order_by(QoS4G.id.asc()).all()
        
        if qoe_records or qos_records:
            has_data = True
            all_weeks = sorted(list(set([r.week_name for r in qoe_records] + [r.week_name for r in qos_records])))
            
            if qoe_records:
                qoe_score_map = {r.week_name: (r.qoe_score or 0) for r in qoe_records}
                qoe_percent_map = {r.week_name: (r.qoe_percent or 0) for r in qoe_records}
                charts['qoe_score_chart'] = {'title': 'Biểu đồ Điểm QoE', 'labels': all_weeks, 'datasets': [{'label': 'Điểm QoE (1-5)', 'data': [qoe_score_map.get(w, None) for w in all_weeks], 'borderColor': '#0078d4', 'fill': False, 'borderWidth': 3}]}
                charts['qoe_percent_chart'] = {'title': 'Biểu đồ Tỷ lệ QoE (%)', 'labels': all_weeks, 'datasets': [{'label': '% QoE', 'data': [qoe_percent_map.get(w, None) for w in all_weeks], 'borderColor': '#107c10', 'fill': False, 'borderWidth': 3}]}
                for r in qoe_records:
                    if r.details:
                        try:
                            d = json.loads(r.details)
                            if not qoe_headers: qoe_headers = list(d.keys())
                            qoe_details.append({'week': r.week_name, 'data': d})
                        except: pass
            
            if qos_records:
                qos_score_map = {r.week_name: (r.qos_score or 0) for r in qos_records}
                qos_percent_map = {r.week_name: (r.qos_percent or 0) for r in qos_records}
                charts['qos_score_chart'] = {'title': 'Biểu đồ Điểm QoS', 'labels': all_weeks, 'datasets': [{'label': 'Điểm QoS (1-5)', 'data': [qos_score_map.get(w, None) for w in all_weeks], 'borderColor': '#ffaa44', 'fill': False, 'borderWidth': 3}]}
                charts['qos_percent_chart'] = {'title': 'Biểu đồ Tỷ lệ QoS (%)', 'labels': all_weeks, 'datasets': [{'label': '% QoS', 'data': [qos_percent_map.get(w, None) for w in all_weeks], 'borderColor': '#e3008c', 'fill': False, 'borderWidth': 3}]}
                for r in qos_records:
                    if r.details:
                        try:
                            d = json.loads(r.details)
                            if not qos_headers: qos_headers = list(d.keys())
                            qos_details.append({'week': r.week_name, 'data': d})
                        except: pass
    gc.collect()
    return render_template('content.html', title="QoE & QoS Analytics", active_page='qoe_qos', cell_name_input=cell_name_input, charts=charts, has_data=has_data, qoe_details=qoe_details, qos_details=qos_details, qoe_headers=qoe_headers, qos_headers=qos_headers)

@app.route('/poi')
@login_required
def poi():
    pname = request.args.get('poi_name', '').strip()
    charts = {}
    pois = []
    try:
        p4 = [r[0] for r in db.session.query(POI4G.poi_name).distinct()]
        p5 = [r[0] for r in db.session.query(POI5G.poi_name).distinct()]
        pois = sorted(list(set(p4 + p5)))
    except: pass
    
    if pname:
        c4 = [r[0] for r in db.session.query(POI4G.cell_code).filter_by(poi_name=pname).all()]
        c5 = [r[0] for r in db.session.query(POI5G.cell_code).filter_by(poi_name=pname).all()]
        
        if c4:
            k4 = db.session.query(KPI4G.thoi_gian, KPI4G.traffic, KPI4G.user_dl_avg_thput).filter(KPI4G.ten_cell.in_(c4)).all()
            if k4:
                try: k4.sort(key=lambda x: datetime.strptime(x.thoi_gian, '%d/%m/%Y'))
                except: pass
                dates4 = sorted(list(set(x.thoi_gian for x in k4)), key=lambda x: datetime.strptime(x, '%d/%m/%Y'))
                agg_traf, agg_thput = defaultdict(float), defaultdict(list)
                for r in k4:
                    if r.thoi_gian in dates4:
                        agg_traf[r.thoi_gian] += (r.traffic or 0)
                        if r.user_dl_avg_thput is not None: agg_thput[r.thoi_gian].append(r.user_dl_avg_thput)

                ds_traf_agg = [{'label': 'Total 4G Traffic (GB)', 'data': [agg_traf[d] for d in dates4], 'borderColor': 'blue', 'fill': False, 'borderWidth': 3, 'spanGaps': True}]
                ds_thput_agg = [{'label': 'Avg 4G Thput (Mbps)', 'data': [(sum(agg_thput[d])/len(agg_thput[d])) if agg_thput[d] else 0 for d in dates4], 'borderColor': 'green', 'fill': False, 'borderWidth': 3, 'spanGaps': True}]

                charts['4g_traf'] = {'title': 'Total 4G Traffic (GB)', 'labels': dates4, 'datasets': ds_traf_agg}
                charts['4g_thp'] = {'title': 'Avg 4G Thput (Mbps)', 'labels': dates4, 'datasets': ds_thput_agg}
        
        if c5:
            k5 = db.session.query(KPI5G.thoi_gian, KPI5G.traffic, KPI5G.user_dl_avg_throughput).filter(KPI5G.ten_cell.in_(c5)).all()
            if k5:
                try: k5.sort(key=lambda x: datetime.strptime(x.thoi_gian, '%d/%m/%Y'))
                except: pass
                dates5 = sorted(list(set(x.thoi_gian for x in k5)), key=lambda x: datetime.strptime(x, '%d/%m/%Y'))
                agg_traf5, agg_thput5 = defaultdict(float), defaultdict(list)
                for r in k5:
                    if r.thoi_gian in dates5:
                        agg_traf5[r.thoi_gian] += (r.traffic or 0)
                        if r.user_dl_avg_throughput is not None: agg_thput5[r.thoi_gian].append(r.user_dl_avg_throughput)
                
                ds_traf_agg5 = [{'label': 'Total 5G Traffic (GB)', 'data': [agg_traf5[d] for d in dates5], 'borderColor': 'orange', 'fill': False, 'borderWidth': 3, 'spanGaps': True}]
                ds_thput_agg5 = [{'label': 'Avg 5G Thput (Mbps)', 'data': [(sum(agg_thput5[d])/len(agg_thput5[d])) if agg_thput5[d] else 0 for d in dates5], 'borderColor': 'purple', 'fill': False, 'borderWidth': 3, 'spanGaps': True}]
                
                charts['5g_traf'] = {'title': 'Total 5G Traffic (GB)', 'labels': dates5, 'datasets': ds_traf_agg5}
                charts['5g_thp'] = {'title': 'Avg 5G Thput (Mbps)', 'labels': dates5, 'datasets': ds_thput_agg5}

    gc.collect()
    return render_template('content.html', title="POI Report", active_page='poi', poi_list=pois, selected_poi=pname, poi_charts=charts)

@app.route('/conges-3g')
@login_required
def conges_3g():
    conges_data, target_dates = [], []
    action = request.args.get('action')
    if action in ['execute', 'export']:
        try:
            all_dates = [d[0] for d in db.session.query(KPI3G.thoi_gian).distinct().all()]
            date_objs = sorted([datetime.strptime(d, '%d/%m/%Y') for d in all_dates if d], reverse=True)
            if len(date_objs) >= 3:
                target_dates = [d.strftime('%d/%m/%Y') for d in date_objs[:3]]
                records = db.session.query(KPI3G.ten_cell, KPI3G.traffic, KPI3G.csconges, KPI3G.pstraffic, KPI3G.psconges).filter(
                    KPI3G.thoi_gian.in_(target_dates),
                    ((KPI3G.csconges > 2) & (KPI3G.cs_so_att > 100)) | ((KPI3G.psconges > 2) & (KPI3G.ps_so_att > 500))
                ).all()
                groups = defaultdict(list)
                for r in records: groups[r.ten_cell].append(r)
                for cell, rows in groups.items():
                    if len(rows) == 3:
                        conges_data.append({
                            'cell_name': cell,
                            'avg_cs_traffic': round(sum(r.traffic or 0 for r in rows)/3, 2),
                            'avg_cs_conges': round(sum(r.csconges or 0 for r in rows)/3, 2),
                            'avg_ps_traffic': round(sum(r.pstraffic or 0 for r in rows)/3, 2),
                            'avg_ps_conges': round(sum(r.psconges or 0 for r in rows)/3, 2)
                        })
        except: pass
    gc.collect()

    if action == 'export':
        df = pd.DataFrame(conges_data)
        output = BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer: df.to_excel(writer, index=False, sheet_name='Congestion 3G')
        output.seek(0)
        return send_file(output, download_name='Congestion3G.xlsx', as_attachment=True)
    return render_template('content.html', title="Congestion 3G", active_page='conges_3g', conges_data=conges_data, dates=target_dates)

@app.route('/worst-cell')
@login_required
def worst_cell():
    duration = int(request.args.get('duration', 1))
    action = request.args.get('action')
    all_dates = [d[0] for d in db.session.query(KPI4G.thoi_gian).distinct().all()]
    date_objs = sorted([datetime.strptime(d, '%d/%m/%Y') for d in all_dates if d], reverse=True)
    target_dates = [d.strftime('%d/%m/%Y') for d in date_objs[:duration]]
    
    results = []
    if target_dates:
        latest_date = target_dates[0]
        l900_cells = {c[0] for c in db.session.query(RF4G.cell_code).filter(RF4G.frequency.ilike('%L900%')).all()}
        active_latest_cells = {c[0] for c in db.session.query(KPI4G.ten_cell).filter(KPI4G.thoi_gian == latest_date).all()}

        records = db.session.query(KPI4G.ten_cell, KPI4G.user_dl_avg_thput, KPI4G.res_blk_dl, KPI4G.cqi_4g, KPI4G.service_drop_all).filter(
            KPI4G.thoi_gian.in_(target_dates),
            ~KPI4G.ten_cell.startswith('MBF_TH'), ~KPI4G.ten_cell.startswith('VNP-4G'),
            ((KPI4G.user_dl_avg_thput < 7000) | (KPI4G.res_blk_dl > 20) | (KPI4G.cqi_4g < 93) | (KPI4G.service_drop_all > 0.3))
        ).all()
    
        groups = defaultdict(list)
        for r in records: 
            if r.ten_cell in active_latest_cells and r.ten_cell not in l900_cells:
                groups[r.ten_cell].append(r)
        
        for cell, rows in groups.items():
            if len(rows) == duration:
                results.append({
                    'cell_name': cell,
                    'avg_thput': round(sum(r.user_dl_avg_thput or 0 for r in rows)/duration, 2),
                    'avg_res_blk': round(sum(r.res_blk_dl or 0 for r in rows)/duration, 2),
                    'avg_cqi': round(sum(r.cqi_4g or 0 for r in rows)/duration, 2),
                    'avg_drop': round(sum(r.service_drop_all or 0 for r in rows)/duration, 2)
                })
                
    gc.collect()
    
    if action == 'export':
        df = pd.DataFrame(results)
        output = BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer: df.to_excel(writer, index=False, sheet_name='Worst Cells')
        output.seek(0)
        return send_file(output, download_name=f'WorstCell_{duration}days.xlsx', as_attachment=True)

    return render_template('content.html', title="Worst Cell", active_page='worst_cell', worst_cells=results, dates=target_dates, duration=duration)

@app.route('/traffic-down')
@login_required
def traffic_down():
    tech = request.args.get('tech', '4g')
    action = request.args.get('action')
    zero_traffic, degraded, degraded_pois, analysis_date = [], [], [], "N/A"
    
    if action in ['execute', 'export_zero', 'export_degraded', 'export_poi_degraded']:
        Model = {'3g': KPI3G, '4g': KPI4G, '5g': KPI5G}.get(tech)
        POI_Model = {'4g': POI4G, '5g': POI5G}.get(tech)
        
        if Model:
            dates_raw = [d[0] for d in db.session.query(Model.thoi_gian).distinct().all()]
            dates_obj = sorted([datetime.strptime(d, '%d/%m/%Y') for d in dates_raw if d], reverse=True)
            if dates_obj:
                latest = dates_obj[0]
                analysis_date = latest.strftime('%d/%m/%Y')
                needed = [latest] + [latest - timedelta(days=i) for i in range(1, 8)]
                needed_str = [d.strftime('%d/%m/%Y') for d in needed]
                
                records = db.session.query(Model.ten_cell, Model.thoi_gian, Model.traffic).filter(Model.thoi_gian.in_(needed_str)).all()
                data_map = defaultdict(dict)
                for r in records:
                    if r.ten_cell.startswith('MBF_TH') or r.ten_cell.startswith('VNP-4G'): continue
                    try: data_map[r.ten_cell][datetime.strptime(r.thoi_gian, '%d/%m/%Y')] = r.traffic or 0
                    except: pass
                
                last_week = latest - timedelta(days=7)
                for cell, d_map in data_map.items():
                    t0 = d_map.get(latest, 0)
                    t_last = d_map.get(last_week, 0)
                    if t0 < 0.1:
                        avg7 = sum(d_map.get(latest - timedelta(days=i), 0) for i in range(1,8)) / 7
                        if avg7 > 2: zero_traffic.append({'cell_name': cell, 'traffic_today': round(t0,3), 'avg_last_7': round(avg7,3)})
                    if t_last > 1 and t0 < 0.7 * t_last:
                        degraded.append({'cell_name': cell, 'traffic_today': round(t0,3), 'traffic_last_week': round(t_last,3), 'degrade_percent': round((1-t0/t_last)*100, 1)})
                
                if POI_Model:
                    poi_map = {r.cell_code: r.poi_name for r in db.session.query(POI_Model).all()}
                    poi_traffic = defaultdict(lambda: {'today': 0, 'last_week': 0})
                    for cell, d_map in data_map.items():
                        if cell in poi_map:
                            p_name = poi_map[cell]
                            poi_traffic[p_name]['today'] += d_map.get(latest, 0)
                            poi_traffic[p_name]['last_week'] += d_map.get(last_week, 0)
                    for pname, traf in poi_traffic.items():
                        t0 = traf['today']
                        t_last = traf['last_week']
                        if t_last > 5 and t0 < 0.7 * t_last:
                             degraded_pois.append({'poi_name': pname, 'traffic_today': round(t0,3), 'traffic_last_week': round(t_last,3), 'degrade_percent': round((1-t0/t_last)*100, 1)})

        gc.collect()

        if action == 'export_zero':
            df = pd.DataFrame(zero_traffic)
            output = BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer: df.to_excel(writer, index=False)
            output.seek(0)
            return send_file(output, download_name=f'ZeroTraffic_{tech}.xlsx', as_attachment=True)
        elif action == 'export_degraded':
            df = pd.DataFrame(degraded)
            output = BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer: df.to_excel(writer, index=False)
            output.seek(0)
            return send_file(output, download_name=f'DegradedTraffic_{tech}.xlsx', as_attachment=True)
        elif action == 'export_poi_degraded':
            df = pd.DataFrame(degraded_pois)
            output = BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer: df.to_excel(writer, index=False)
            output.seek(0)
            return send_file(output, download_name=f'POIDegraded_{tech}.xlsx', as_attachment=True)

    return render_template('content.html', title="Traffic Down", active_page='traffic_down', zero_traffic=zero_traffic, degraded=degraded, degraded_pois=degraded_pois, tech=tech, analysis_date=analysis_date)

@app.route('/rf')
@login_required
def rf():
    tech = request.args.get('tech', '4g')
    action = request.args.get('action')
    search_query = request.args.get('cell_search', '').strip()
    Model = {'3g': RF3G, '4g': RF4G, '5g': RF5G}.get(tech)
    
    query = Model.query
    if search_query:
        query = query.filter(or_(Model.cell_code.ilike(f"%{search_query}%"), Model.site_code.ilike(f"%{search_query}%")))
        
    if action == 'export':
        def generate():
            yield '\ufeff'.encode('utf-8')
            cols = [c.key for c in Model.__table__.columns if c.key not in ['id', 'extra_data']]
            yield (','.join(cols) + '\n').encode('utf-8')
            seen_export = set()
            for row in query.all():
                if row.cell_code not in seen_export:
                    seen_export.add(row.cell_code)
                    yield (','.join([str(getattr(row, c) or '').replace(',', ';') for c in cols]) + '\n').encode('utf-8')
        return Response(stream_with_context(generate()), mimetype='text/csv', headers={"Content-Disposition": f"attachment; filename=RF_{tech}.csv"})

    rows = query.all()
    cols = [c.key for c in Model.__table__.columns if c.key not in ['id', 'extra_data']]
    data = []
    seen_cells = set()
    
    for r in rows:
        if r.cell_code not in seen_cells:
            seen_cells.add(r.cell_code)
            data.append({c: getattr(r, c) for c in cols} | {'id': r.id})
            if not search_query and len(data) >= 100:
                break
                
    return render_template('content.html', title="RF Database", active_page='rf', current_tech=tech, rf_columns=cols, rf_data=data, search_query=search_query)

@app.route('/rf/delete/<tech>/<int:id>')
@login_required
def rf_delete(tech, id):
    if current_user.role != 'admin': return redirect(url_for('rf', tech=tech))
    Model = {'3g': RF3G, '4g': RF4G, '5g': RF5G}.get(tech)
    db.session.delete(db.session.get(Model, id))
    db.session.commit()
    flash('Đã xóa', 'success')
    return redirect(url_for('rf', tech=tech))

@app.route('/rf/detail/<tech>/<int:id>')
@login_required
def rf_detail(tech, id):
    Model = {'3g': RF3G, '4g': RF4G, '5g': RF5G}.get(tech)
    obj = db.session.get(Model, id)
    clean_obj = {k: v for k, v in obj.__dict__.items() if not k.startswith('_')}
    return render_template('rf_detail.html', obj=clean_obj, tech=tech)

@app.route('/rf/add', methods=['GET', 'POST'])
@login_required
def rf_add():
    if current_user.role != 'admin': return redirect(url_for('rf', tech=request.args.get('tech', '3g')))
    tech = request.args.get('tech', '3g')
    Model = {'3g': RF3G, '4g': RF4G, '5g': RF5G}.get(tech)
    if request.method == 'POST':
        data = {k: v for k, v in request.form.items() if k in Model.__table__.columns.keys()}
        db.session.add(Model(**data)); db.session.commit(); flash('Added', 'success')
        return redirect(url_for('rf', tech=tech))
    cols = [c.key for c in Model.__table__.columns if c.key != 'id']
    return render_template('rf_form.html', title=f"Add RF {tech}", columns=cols, tech=tech, obj={})

@app.route('/rf/edit/<tech>/<int:id>', methods=['GET', 'POST'])
@login_required
def rf_edit(tech, id):
    if current_user.role != 'admin': return redirect(url_for('rf', tech=tech))
    Model = {'3g': RF3G, '4g': RF4G, '5g': RF5G}.get(tech)
    obj = db.session.get(Model, id)
    if request.method == 'POST':
        for k,v in request.form.items(): setattr(obj, k, v)
        db.session.commit(); flash('Updated', 'success'); return redirect(url_for('rf', tech=tech))
    cols = [c.key for c in Model.__table__.columns if c.key != 'id']
    return render_template('rf_form.html', title=f"Edit RF {tech}", columns=cols, tech=tech, obj=obj.__dict__)

@app.route('/script', methods=['GET', 'POST'])
@login_required
def script():
    script_result = ""
    if request.method == 'POST':
        tech = request.form.get('tech')
        rns = request.form.getlist('rn[]')
        srns = request.form.getlist('srn[]')
        hsns = request.form.getlist('hsn[]')
        hpns = request.form.getlist('hpn[]')
        rcns = request.form.getlist('rcn[]')
        secids = request.form.getlist('sectorid[]')
        rxnums = request.form.getlist('rxnum[]')
        txnums = request.form.getlist('txnum[]')

        lines = []
        for i in range(len(rns)):
            lines.append(f"ADD RRUCHAIN: RCN={rcns[i]}, TT=CHAIN, BM=COLD, AT=LOCALPORT, HSRN=0, HSN={hsns[i]}, HPN={hpns[i]}, CR=AUTO, USERDEFRATENEGOSW=OFF;")
            rs_mode = "GU" if "900" in tech else "UO" if "2100" in tech else "LO"
            if tech == '4g': rs_mode = "LO"
            lines.append(f"ADD RRU: CN=0, SRN={srns[i]}, SN=0, TP=TRUNK, RCN={rcns[i]}, PS=0, RT=MRRU, RS={rs_mode}, RN={rns[i]}, RXNUM={rxnums[i]}, TXNUM={txnums[i]}, MNTMODE=NORMAL, RFDCPWROFFALMDETECTSW=OFF, RFTXSIGNDETECTSW=OFF;")
            
            ant_num = rxnums[i]
            ant_str = f"ANT1CN=0, ANT1SRN={srns[i]}, ANT1SN=0, ANT1N=R0A"
            if int(ant_num) >= 2: ant_str += f", ANT2CN=0, ANT2SRN={srns[i]}, ANT2SN=0, ANT2N=R0B"
            if int(ant_num) >= 4: ant_str += f", ANT3CN=0, ANT3SRN={srns[i]}, ANT3SN=0, ANT3N=R0C, ANT4CN=0, ANT4SRN={srns[i]}, ANT4SN=0, ANT4N=R0D"
            lines.append(f"ADD SECTOR: SECTORID={secids[i]}, ANTNUM={ant_num}, {ant_str}, CREATESECTOREQM=FALSE;")
            
            ant_type_str = "ANTTYPE1=RXTX_MODE"
            if int(ant_num) >= 2: ant_type_str += ", ANTTYPE2=RXTX_MODE"
            if int(ant_num) >= 4: ant_type_str += ", ANTTYPE3=RXTX_MODE, ANTTYPE4=RXTX_MODE"
            lines.append(f"ADD SECTOREQM: SECTOREQMID={secids[i]}, SECTORID={secids[i]}, ANTCFGMODE=ANTENNAPORT, ANTNUM={ant_num}, {ant_str.replace(f'ANT1SRN={srns[i]}', 'ANT1SRN=0').replace(f'ANT2SRN={srns[i]}', 'ANT2SRN=0').replace(f'ANT3SRN={srns[i]}', 'ANT3SRN=0').replace(f'ANT4SRN={srns[i]}', 'ANT4SRN=0')}, {ant_type_str};")
            lines.append("") 

        script_result = "\n".join(lines)

    return render_template('content.html', title="Generate Script", active_page='script', script_result=script_result)

@app.route('/backup', methods=['POST'])
@login_required
def backup_db():
    if current_user.role != 'admin': return redirect(url_for('index'))
    selected_tables = request.form.getlist('tables')
    if not selected_tables: return redirect(url_for('backup_restore'))
    stream = BytesIO()
    models_map = {'users.csv': User, 'cell3g.csv': Cell3G, 'config3g.csv': Config3G, 'rf3g.csv': RF3G, 'rf4g.csv': RF4G, 'rf5g.csv': RF5G, 'poi4g.csv': POI4G, 'poi5g.csv': POI5G, 'kpi3g.csv': KPI3G, 'kpi4g.csv': KPI4G, 'kpi5g.csv': KPI5G, 'qoe_4g.csv': QoE4G, 'qos_4g.csv': QoS4G}
    with zipfile.ZipFile(stream, 'w', zipfile.ZIP_DEFLATED) as zf:
        for fname in selected_tables:
            if fname in models_map:
                Model = models_map[fname]
                cols = [c.key for c in Model.__table__.columns]
                data = db.session.query(Model).all()
                df = pd.DataFrame([{c: getattr(row, c) for c in cols} for row in data]) if data else pd.DataFrame(columns=cols)
                zf.writestr(fname, df.to_csv(index=False, encoding='utf-8-sig'))
    stream.seek(0); gc.collect()
    return send_file(stream, as_attachment=True, download_name=f'backup_{datetime.now().strftime("%Y%m%d")}.zip')

@app.route('/restore', methods=['POST'])
@login_required
def restore_db():
    if current_user.role != 'admin': return redirect(url_for('index'))
    file = request.files['file']
    if file:
        try:
            file_bytes = BytesIO(file.read())
            with zipfile.ZipFile(file_bytes) as zf:
                models = {'users.csv': User, 'cell3g.csv': Cell3G, 'config3g.csv': Config3G, 'rf3g.csv': RF3G, 'rf4g.csv': RF4G, 'rf5g.csv': RF5G, 'poi4g.csv': POI4G, 'poi5g.csv': POI5G, 'kpi3g.csv': KPI3G, 'kpi4g.csv': KPI4G, 'kpi5g.csv': KPI5G, 'qoe_4g.csv': QoE4G, 'qos_4g.csv': QoS4G}
                for fname in zf.namelist():
                    if fname in models:
                        Model = models[fname]
                        with zf.open(fname) as f: df = pd.read_csv(f, encoding='utf-8-sig')
                        db.session.query(Model).delete()
                        records = [{k: (v if not pd.isna(v) else None) for k, v in r.items() if k in [c.key for c in Model.__table__.columns]} for r in df.to_dict('records')]
                        if records: db.session.bulk_insert_mappings(Model, records)
                db.session.commit(); flash('Restore Success', 'success')
        except Exception as e: db.session.rollback(); flash(f'Error: {e}', 'danger')
    return redirect(url_for('backup_restore'))

@app.route('/backup-restore')
@login_required
def backup_restore(): return render_template('backup_restore.html', title="Backup", active_page='backup_restore')

@app.route('/users')
@login_required
def manage_users(): return render_template('user_management.html', users=User.query.all(), active_page='users')

@app.route('/users/add', methods=['POST'])
@login_required
def add_user(): 
    u = User(username=request.form['username'], role=request.form['role']); u.set_password(request.form['password'])
    db.session.add(u); db.session.commit(); return redirect(url_for('manage_users'))

@app.route('/users/delete/<int:id>')
@login_required
def delete_user(id): db.session.delete(db.session.get(User, id)); db.session.commit(); return redirect(url_for('manage_users'))

@app.route('/profile')
@login_required
def profile(): return render_template('profile.html', active_page='profile')

@app.route('/change-password', methods=['POST'])
@login_required
def change_password(): 
    if current_user.check_password(request.form['current_password']): current_user.set_password(request.form['new_password']); db.session.commit(); flash('Done', 'success')
    return redirect(url_for('profile'))

if __name__ == '__main__':
    app.run(debug=True)
