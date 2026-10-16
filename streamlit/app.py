"""Run from the repository: poetry run streamlit run streamlit/app.py"""
from datetime import datetime, timezone
import os
from pathlib import Path
import sys

import altair as alt
import duckdb
import streamlit as st

# Also supports Streamlit AppTest and launches from another working directory.
sys.path.insert(0, str(Path(__file__).resolve().parent))
from reports import DEFAULT_DATABASE, ROOT, load_metadata, load_report

st.set_page_config(page_title='EventSim | Music insights', page_icon='🎧', layout='wide')
st.html('''<style>
    .stMainBlockContainer {max-width: 1440px; padding-top: 2rem;}
    [data-testid="stMetric"] {background: #f0fdfa; border: 1px solid #ccfbf1;
        border-radius: 12px; padding: 16px; color: #134e4a;}
    h1 {letter-spacing: -0.035em;}
    [data-testid="stSidebar"] {border-right: 1px solid #e2e8f0;}
</style>''')
TEAL = '#0d9488'
PURPLE = '#7c3aed'
DAYS = ['Thứ Hai', 'Thứ Ba', 'Thứ Tư', 'Thứ Năm', 'Thứ Sáu', 'Thứ Bảy', 'Chủ nhật']


@st.cache_data(ttl=60, show_spinner=False)
def metadata_cached(path, version):
    return load_metadata(path)


@st.cache_data(ttl=60, show_spinner=False)
def report_cached(path, version, start, end, states, genders):
    return load_report(path, start, end, states, genders)


def number(value, decimals=0):
    return f'{value:,.{decimals}f}'.replace(',', '_').replace('.', ',').replace('_', '.')


def bar_chart(frame, category, color=TEAL, title=''):
    if frame.empty:
        st.info('Chưa có dữ liệu xác định cho bảng xếp hạng này.')
        return
    chart = alt.Chart(frame).mark_bar(cornerRadiusEnd=5, color=color).encode(
        x=alt.X('plays:Q', title='Lượt nghe', axis=alt.Axis(tickMinStep=1)),
        y=alt.Y(f'{category}:N', title=None, sort='-x', axis=alt.Axis(labelLimit=420)),
        tooltip=[alt.Tooltip(f'{category}:N', title=title or 'Nhóm'),
                 alt.Tooltip('plays:Q', title='Lượt nghe', format=',')],
    ).properties(height=230)
    st.altair_chart(chart, width='stretch')


def main():
    st.caption('EVENTSIM  /  MUSIC ANALYTICS')
    st.title('🎧 Nhịp điệu người nghe')
    st.write('Khám phá nơi nghe nhạc, bài hát được yêu thích và thói quen nghe trong tuần.')

    with st.sidebar:
        st.header('Bộ lọc báo cáo')
        database = st.text_input('DuckDB database', value=os.getenv(
            'DASHBOARD_DUCKDB_PATH', str(DEFAULT_DATABASE)), key='database')
        refresh = st.button('↻ Làm mới dữ liệu', key='refresh', width='stretch')
        st.caption('Làm mới sau khi ingestion và dbt build thành công.')
    if refresh:
        metadata_cached.clear()
        report_cached.clear()

    path = Path(database).expanduser()
    if not path.is_absolute():
        path = ROOT / path
    try:
        # Detect normal file replacement; button also reloads changes still in a WAL.
        version = path.stat().st_mtime_ns if path.is_file() else 0
        meta = metadata_cached(str(path), version)
    except (duckdb.Error, OSError, ValueError) as exc:
        st.error('Không đọc được bảng dashboard. Kiểm tra đường dẫn DuckDB, các bảng dashboard và '
                 'đóng writer/DBeaver trước khi làm mới. Hãy chạy dbt build tầng dashboard thành công trước khi xem báo cáo.')
        with st.expander('Chi tiết lỗi'):
            st.code(str(exc))
        return
    if meta['min_date'] is None:
        st.info('Bảng dashboard chưa có lượt nghe với thời điểm bắt đầu hợp lệ. Nạp dữ liệu và chạy dbt build, rồi làm mới.')
        return

    with st.sidebar:
        dates = st.date_input('Khoảng ngày (UTC)',
                              value=(meta['min_date'], meta['max_date']),
                              min_value=meta['min_date'], max_value=meta['max_date'], key='dates')
        states = st.multiselect('Tiểu bang', meta['states'], key='states', placeholder='Tất cả tiểu bang')
        genders = st.multiselect('Giới tính', meta['genders'], key='genders', placeholder='Tất cả giới tính')
        st.caption('Để trống bộ lọc tiểu bang/giới tính để chọn tất cả, gồm Unknown.')
        st.divider()
        st.caption('Múi giờ: UTC · Xếp hạng: số lượt nghe')
        st.caption('Nguồn: dashboard.song_plays và dashboard.filter_options.')
    if not isinstance(dates, (tuple, list)) or len(dates) != 2:
        st.info('Chọn đủ ngày bắt đầu và ngày kết thúc.')
        return
    start, end = dates
    try:
        with st.spinner('Đang tổng hợp báo cáo từ bảng dashboard…'):
            report = report_cached(str(path), version, start, end, tuple(states), tuple(genders))
    except (duckdb.Error, OSError, ValueError) as exc:
        st.error('Không tải được báo cáo. Chờ ingestion/dbt hoàn tất, đóng kết nối writer rồi bấm Làm mới dữ liệu.')
        with st.expander('Chi tiết lỗi'):
            st.code(str(exc))
        return

    audit = meta['updated_at']
    event = meta['latest_event']
    st.caption(f"{start:%d/%m/%Y} → {end:%d/%m/%Y} · "
               f"Fact cập nhật gần nhất: {audit} UTC · Event gần nhất: {event} UTC")
    if meta['missing_timestamp']:
        st.caption(f"Có {number(meta['missing_timestamp'])} lượt thiếu timestamp, không tham gia báo cáo theo ngày/giờ.")
    kpi = report['summary'].iloc[0]
    total = int(kpi['plays'])
    if total == 0:
        st.info('Không có lượt nghe trong phạm vi bộ lọc. Hãy mở rộng ngày, tiểu bang hoặc giới tính.')
        return
    columns = st.columns(4)
    for col, label, value in zip(columns,
                                ['Lượt nghe', 'Người nghe', 'Bài hát', 'Giờ nghe ước tính'],
                                [number(total), number(kpi['listeners']), number(kpi['songs']),
                                 number(kpi['known_seconds'] / 3600, 2)]):
        col.metric(label, value)
    coverage = (total - kpi['duration_missing']) / total * 100
    st.caption(f"Thời lượng có dữ liệu: {coverage:.1f}% lượt nghe · "
               f"{number(kpi['duration_missing'])} lượt thiếu thời lượng. Người nghe được đếm theo user_id, gộp các phiên bản SCD2.")
    st.divider()

    left, right = st.columns(2, gap='large')
    with left:
        st.subheader('Top 5 tiểu bang')
        bar_chart(report['states'], 'state', title='Tiểu bang')
        st.caption(f"Loại {number(kpi['unknown_location'])} lượt có tiểu bang Unknown khỏi bảng xếp hạng.")
    with right:
        st.subheader('Top 5 bài hát')
        songs = report['songs'].copy()
        songs['label'] = [f'{i + 1}. {r.song} — {r.artist}' for i, r in enumerate(songs.itertuples())]
        bar_chart(songs, 'label', PURPLE, 'Bài hát / nghệ sĩ')
        st.caption(f"Loại {number(kpi['unknown_song'])} lượt thiếu song_key khỏi bảng xếp hạng. Mỗi bài theo khóa dim_song.")

    st.subheader('Thời gian nghe theo giờ trong tuần')
    metric = st.radio('Đơn vị heatmap', ['Giờ nghe ước tính', 'Lượt nghe'], horizontal=True, key='heat_metric')
    hourly = report['hourly'].copy()
    hourly['day'] = hourly['weekday'].map(dict(enumerate(DAYS, 1)))
    hourly['hours'] = hourly['known_seconds'] / 3600
    measure = 'hours' if metric == 'Giờ nghe ước tính' else 'plays'
    chart = alt.Chart(hourly).mark_rect(cornerRadius=3).encode(
        x=alt.X('hour:O', title='Giờ bắt đầu lượt nghe (UTC)', sort=list(range(24))),
        y=alt.Y('day:N', title=None, sort=DAYS),
        color=alt.Color(f'{measure}:Q', title=metric, scale=alt.Scale(scheme='tealblues')),
        tooltip=[alt.Tooltip('day:N', title='Thứ'), alt.Tooltip('hour:O', title='Giờ UTC'),
                 alt.Tooltip('plays:Q', title='Lượt nghe'),
                 alt.Tooltip('hours:Q', title='Giờ ước tính', format='.2f'),
                 alt.Tooltip('measured_plays:Q', title='Lượt có thời lượng')],
    ).properties(height=280)
    st.altair_chart(chart, width='stretch')
    st.caption('Gộp các tuần trong khoảng ngày đã chọn. Toàn bộ thời lượng được gán vào giờ bắt đầu '
               'lượt nghe, không chia qua các giờ. Ô 0 có thể không có lượt hoặc thiếu thời lượng; xem tooltip.')

    st.subheader('Bài hát nghe nhiều nhất theo giới tính')
    gender = report['gender_songs'].rename(columns={
        'gender': 'Giới tính', 'song': 'Bài hát', 'artist': 'Nghệ sĩ', 'plays': 'Lượt nghe'})
    st.dataframe(gender[['Giới tính', 'Bài hát', 'Nghệ sĩ', 'Lượt nghe']], hide_index=True, width='stretch')
    st.caption('M/F giữ nguyên mã nguồn. Unknown là giới tính chưa xác định. Khi đồng hạng, chọn '
               'theo tên bài, nghệ sĩ rồi song_key để kết quả ổn định.')

    st.subheader('Thêm góc nhìn')
    trend, artists = st.columns(2, gap='large')
    with trend:
        st.write('**Xu hướng lượt nghe theo ngày**')
        st.altair_chart(alt.Chart(report['daily']).mark_line(point=True, color=TEAL).encode(
            x=alt.X('date:T', title='Ngày UTC'), y=alt.Y('plays:Q', title='Lượt nghe'),
            tooltip=[alt.Tooltip('date:T', title='Ngày'), alt.Tooltip('plays:Q', title='Lượt nghe')],
        ).properties(height=220), width='stretch')
    with artists:
        st.write('**Top 5 nghệ sĩ**')
        bar_chart(report['artists'], 'artist', PURPLE, 'Nghệ sĩ')
    peak = hourly.sort_values(['plays', 'weekday', 'hour'], ascending=[False, True, True]).iloc[0]
    st.info(f"Khung giờ nhiều lượt nhất: **{peak['day']}, {int(peak['hour']):02d}:00 UTC**, "
            f"với **{number(peak['plays'])} lượt** ({peak['plays'] / total:.1%} trong phạm vi đã chọn).")
    if not report['songs'].empty:
        winner = report['songs'].iloc[0]
        st.write(f"Bài dẫn đầu: **{winner['song']} — {winner['artist']}**, "
                 f"chiếm **{winner['plays'] / total:.1%}** tổng lượt nghe đã chọn.")
    with st.expander('Cách đọc số liệu và giới hạn'):
        st.write('Một dòng fact là một lượt nghe. listened_seconds là khoảng cách đến event tiếp theo '
                 'trong cùng phiên, có thể gồm khoảng nghỉ và không bị giới hạn bởi độ dài bài hát. '
                 'Đây là thời lượng ước tính, không phải bằng chứng nghe hết bài. NULL không được '
                 'suy diễn thành thời lượng thực bằng 0; tổng chỉ cộng các giá trị đã biết.')
        st.write('Dashboard tổng hợp từ bảng dashboard đã được dbt tiền xử lý, giữ chính sách thời lượng của warehouse. '
                 'Top tiểu bang/bài hát bỏ nhóm Unknown tương ứng, còn tổng lượt và heatmap vẫn giữ chúng. '
                 'Giới tính được lookup bằng user_key của phiên bản SCD2 tại lúc nghe.')
        st.write('Kết nối DuckDB chỉ đọc và được đóng sau mỗi lần tải. Nếu một process khác đang ghi, '
                 'lượt tải có thể bị khóa; chờ writer kết thúc và làm mới. Dashboard không tự chạy dbt '
                 'và không chứng nhận build đã thành công. Chỉ refresh dữ liệu sau dbt build thành công.')
    st.caption('Hiển thị báo cáo lúc ' + datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC') +
               ' · Cache tối đa 60 giây; nút làm mới bỏ cache.')


main()
