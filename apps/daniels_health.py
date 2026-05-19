# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "altair==6.1.0",
#     "apple-health==2.0.0",
#     "garminconnect==0.3.3",
#     "ipython==9.13.0",
#     "lxml==5.4.0",
#     "marimo[lsp,openai]",
#     "openai==2.33.0",
#     "pandas==2.2.3",
#     "persist-cache==0.4.4",
#     "polars==1.29.0",
#     "pyarrow",
#     "pydantic-ai-slim[openai]==1.89.1",
#     "python-dotenv==1.1.0",
#     "requests==2.32.3",
#     "wat==0.6.0",
# ]
# ///

import marimo

__generated_with = "0.23.6"
app = marimo.App(
    width="columns",
    layout_file="layouts/daniels_health.grid.json",
)


@app.cell(column=0, hide_code=True)
def _(mo):
    mo.md(r"""
    ## Daniels hälsostatistik 📈

    Samlad data från olika källor (Garmin, Apple och Jefit) med visuella grafer som jag finner mest värde.

    För mer detaljer gå [hit](https://github.com/engdan77/notebooks) eller för andra utvecklade projekt besök [Daniels Github](https://github.com/engdan77).

    💾 Källkod: [här](https://github.com/engdan77/notebooks/blob/main/apps/daniels_health.py) (2025.5.1)
    ✉️ E-post: [daniel@engvalls.eu](mailto:daniel@engvalls.eu)
    """)
    return


@app.cell(hide_code=True)
def check_if_locally():
    import marimo as mo
    from pathlib import Path
    running_locally = isinstance(mo.notebook_location(), Path)
    return Path, mo, running_locally


@app.cell(hide_code=True)
def imports_and_global_funcs(logger, mo, running_locally):
    import polars as pl
    import altair as alt
    import json
    import os
    import datetime
    import sys
    from pyarrow import parquet as pq
    import re

    ALTAIR_WIDTH = 900


    def is_mobile():
        # Currently not working with WASM so skipping
        try:
            from pyodide.code import run_js
        except ModuleNotFoundError:
            return False
        ug = run_js("navigator.userAgent")
        return bool(re.match('.*?Mobile|iP(hone|od|ad)|Android|BlackBerry|IEMobile.*', ug))


    def is_wasm() -> bool:
        return "pyodide" in sys.modules

    if not is_wasm():
        mo.output.append(mo.md('Kör lokalt på dator'))
        exec('''
    import dotenv
    from tempfile import NamedTemporaryFile
    from zipfile import ZipFile
    from health import HealthData
    import wat
    ''')
    else:
        import pyodide
        mo.output.append(mo.md('Kör som WASM'))


    def file_exists(loc) -> bool:
        # Check if the Path (locally) or URLPath (remote) exists
        if not running_locally:
            return True  # Assume if run as WASM that file exists
        return loc.exists()


    async def async_get(loc):
        r = await pyodide.http.pyfetch(loc)
        c = await r.bytes()
        logger.info(f'Laddade ned {loc} med storleken {len(c)} bytes')
        return c


    async def read_df(loc) -> pl.LazyFrame:
        # Due to need workaround for remotely loading Parquete files - https://github.com/pola-rs/polars/issues/20876
        _df = None
        if not running_locally:
            mo.output.append(mo.md('Hämtar data från URL'))
            c = await async_get(loc)
            with open('data.parquet', 'wb') as f:
                f.write(c)
            try:
                table = pq.read_table('data.parquet')
            except Exception:
                logger.error(f'Unable to read downloaded parquet file {loc}')
                return None
            _df = pl.from_arrow(table).lazy()
        else: 
            _df = pl.scan_parquet(loc)
        return _df


    def to_alt_dt(dt: datetime.datetime | datetime.date | str) -> alt.DateTime:
        if isinstance(dt, str):
            _dt = datetime.datetime.fromisoformat(dt)
        elif isinstance(dt, datetime.date):
            _dt = datetime.datetime(dt.year, dt.month, dt.day)
        elif isinstance(dt, datetime.datetime):
            _dt = dt
        return alt.DateTime(year=_dt.year, month=_dt.month, date=_dt.day, hours=_dt.hour, minutes=_dt.minute)

    return (
        ALTAIR_WIDTH,
        alt,
        datetime,
        file_exists,
        is_mobile,
        is_wasm,
        os,
        pl,
        read_df,
        to_alt_dt,
    )


@app.cell(hide_code=True)
def define_relevant_columns(mo):
    relevant_apple_colums = [
      "dt",
      "metric",
      "unit",
      "value", 
    ]

    relevant_garmin_colums = [
        "dt",
        "activityId",
        "startTimeLocal",
        "distance",
        "duration",
        "calories",
        "steps",
        "activityType.typeKey",
        "activityName",
        "secsInZone1",
        "secsInZone2",
        "secsInZone3",
        "secsInZone4",
        "secsInZone5",
    ]

    relevant_gym_columns = [
        'dt',
        'exercise',
        'rep_max',
        'sets'
    ]

    def padded(l: list, by: int):
        _x = [''] * by
        for i, item in enumerate(l):
            _x[i] = item
        return _x


    relevant_columns_dict = {'Garmin': relevant_garmin_colums, 
                             'Apple': padded(relevant_apple_colums, by=len(relevant_garmin_colums)), 
                             'Gym': padded(relevant_gym_columns, by=len(relevant_garmin_colums))}
    mo.output.append(mo.md('### Kolumner som används för data'))
    mo.output.append(mo.ui.table(relevant_columns_dict, page_size=30))
    return relevant_apple_colums, relevant_garmin_colums


@app.cell(hide_code=True)
def define_global_vars(mo):
    garmin_file = mo.notebook_location() / 'public' / 'all_health_garmin.parquet'
    mo.output.append(mo.md(f'Garmin fil att använda `{garmin_file}`'))
    apple_file = mo.notebook_location() / 'public' / 'apple_health.parquet'
    mo.output.append(mo.md(f'Apple Hälsa fil att använda `{apple_file}`'))
    jefit_file = mo.notebook_location() / 'public' / 'jefit_health.parquet'
    mo.output.append(mo.md(f'Jefit (Gym) Hälsa fil att använda `{jefit_file}`'))
    return apple_file, garmin_file, jefit_file


@app.cell(hide_code=True)
def form_for_display(datetime, mo):
    interval_categories = {'dag': '1d', 'vecka': '1w', 'månad': '1mo', 'år': '1y'}

    _start_date = datetime.date(datetime.date.today().year - 3, 1, 1)
    _end_date = datetime.date.today()

    form = mo.md('''
    ### Ange detaljer för statistik

    Mellan datum: {date_range}

    Gruppera per {interval_input}  ... (upplösning)

    ''').batch(
        date_range=mo.ui.date_range(value=(_start_date, _end_date)),
        interval_input=mo.ui.dropdown(interval_categories, value='månad')
    )

    form
    return form, interval_categories


@app.cell(hide_code=True)
def select_activity_for_pulse_zones_chart(activities_dist, mo):
    activity_types = activities_dist.select('activity').to_series().to_list()
    _kwargs = {'value': 'running'} if 'running' in activity_types else {}
    _form_text = mo.md('Se aktiviteter av typ ... {activity_input} ...')
    activity_type_form = _form_text.batch(activity_input=mo.ui.dropdown(activity_types, **_kwargs))
    activity_type_form
    return (activity_type_form,)


@app.cell(hide_code=True)
def get_data_periods_gantt(
    all_apple_df,
    all_garmin_df,
    end_date,
    jefit_df,
    mo,
    pl,
    start_date,
):
    _chosen_years = end_date.year - start_date.year
    if not _chosen_years:
        _chosen_years = 1

    _apple_stats = all_apple_df.select(min_dt=pl.col('dt').min(), max_dt=pl.col('dt').max()).collect()
    _apple_start = _apple_stats.get_column('min_dt')[0].year
    _apple_years = _apple_stats.get_column('max_dt')[0].year - _apple_start

    _garmin_stats = all_garmin_df.select(min_dt=pl.col('dt').min(), max_dt=pl.col('dt').max()).collect()
    _garmin_start = _garmin_stats.get_column('min_dt')[0].year
    _garmin_years = _garmin_stats.get_column('max_dt')[0].year - _garmin_start

    _jefit_stats = jefit_df.select(min_dt=pl.col('dt').min(), max_dt=pl.col('dt').max()).collect()
    _jefit_start = _jefit_stats.get_column('min_dt')[0].year
    _jefit_years = _jefit_stats.get_column('max_dt')[0].year - _jefit_start

    _gantt = f'''
    gantt
        title Data över år
        Vald period: crit, {start_date.year}, {_chosen_years}y
        Apple: {_apple_start}, {_apple_years}y
        Garmin: {_garmin_start}, {_garmin_years}y
        Gym: {_jefit_start}, {_jefit_years}y
    '''
    mo.mermaid(_gantt)
    return


@app.cell(hide_code=True)
def set_month_text(interval_categories, interval_input, mo):
    mo.stop(interval_input is None)
    month_text = [k for k, v in interval_categories.items() if v == interval_input].pop()

    bar_width = {'dag': 3, 'vecka': 5, 'månad': 5, 'år': 15}.get(month_text, 'dag')
    return bar_width, month_text


@app.cell(hide_code=True)
async def load_or_empty_current_garmin_data(
    end_date,
    file_exists,
    garmin_file,
    is_mobile,
    logger,
    mo,
    pl,
    read_df,
    relevant_garmin_colums,
    start_date,
):
    if file_exists(garmin_file):
        if is_mobile():
            _m = 'Du kör från en mobil med begränsat minne, kör Chrome från dator.'
            logger.warning(_m)
            mo.stop(True, 'Du kör från en mobil med begränsat minne, kör Chrome från dator.')
        else:
            logger.info('You are running WASM from a computer browser')
        _df = await read_df(garmin_file)
        all_garmin_df = _df
        current_garmin_data = _df.filter(pl.col('dt').is_between(start_date, end_date))

        # Collect only necessary stats for display
        _stats = _df.select(
            min_dt=pl.col('dt').min(),
            max_dt=pl.col('dt').max(),
            count=pl.len()
        ).collect()

        _min_dt = _stats.get_column('min_dt')[0]
        _max_dt = _stats.get_column('max_dt')[0]
        _count = _stats.get_column('count')[0]

        mo.output.append(mo.Html(f'''<u><b>Garmin data</b/></u>Period: {_min_dt:%Y-%m-%d} - {_max_dt:%Y-%m-%d} [{_count} rader]</br>''')) 
    else:
        current_garmin_data = pl.DataFrame({k: [] for k in relevant_garmin_colums}).lazy()
        all_garmin_df = current_garmin_data
    return all_garmin_df, current_garmin_data


@app.cell(hide_code=True)
def get_activities_as_chart(ALTAIR_WIDTH, alt, current_garmin_data, pl):
    activities_dist = current_garmin_data.rename({"activityType.typeKey": 'activity'}).group_by(pl.col('activity')).agg(pl.len().alias('count')).collect()
    _chart = activities_dist.plot.bar(y='activity:N', x='count:Q').encode(x=alt.X('count', scale=alt.Scale(domainMin=0))).properties(height=100, title='Antal aktiviteter av typ').properties(width=ALTAIR_WIDTH).interactive(bind_x=False, bind_y=False)

    _chart
    return (activities_dist,)


@app.cell(hide_code=True)
def get_chart_zones_and_temp(
    ALTAIR_WIDTH,
    activity_type_form,
    alt,
    chart_data_mins_per_km,
    end_date,
    interval_median_zones_chart,
    mo,
    pl,
    start_date,
    to_alt_dt,
):
    mo.stop(any(_ is None for _ in activity_type_form.value.values()) is True, mo.md('Välj aktivitet för zoner'))

    _stats = chart_data_mins_per_km.select(
        min_v=pl.col('mean_mins_per_km').min(),
        max_v=pl.col('mean_mins_per_km').max()
    ).collect()

    min_tempo = _stats.get_column('min_v')[0] - 0.5
    max_tempo = _stats.get_column('max_v')[0] + 0.5

    median_km_per_hour_chart = alt.Chart(chart_data_mins_per_km.filter(pl.col('dt_interval') >= start_date).collect()).mark_line(
        strokeWidth=5,
        color='red',
        interpolate="monotone"
        ).encode(
        x=alt.X('dt_interval:T', scale=alt.Scale(domain=[to_alt_dt(start_date), to_alt_dt(end_date)])),
        y=alt.Y('mean_mins_per_km', scale=alt.Scale(domain=[min_tempo, max_tempo]))   
    ).properties(
        title='Median min/km hastighet för aktivitet (tempo)',
        width=ALTAIR_WIDTH,
        height=200,

    )
    interval_median_zones_chart & median_km_per_hour_chart
    return


@app.cell(hide_code=True)
def get_count_distances_chart(
    ALTAIR_WIDTH,
    activity_input,
    alt,
    bar_width,
    current_garmin_data,
    end_date,
    interval_input,
    month_text,
    pl,
    start_date,
    to_alt_dt,
):
    _colors = {'<3km': 'green', 
               '3-5km': 'yellow', 
               '6-10km': 'orange', 
               '>10km': 'red'}

    _activity_counts = (
        current_garmin_data.filter(pl.col('activityType.typeKey').eq(activity_input))
        .with_columns(
            (pl.col("distance") / 1000).alias("distance_km"),  # Convert distance to kilometers
            pl.col("dt").dt.truncate(interval_input).alias("dt_interval")  # Extract month from datetime
        )
        .with_columns(
            pl.when(pl.col("distance_km") < 2.8).then(pl.lit("<3km"))
            .when((pl.col("distance_km") >= 2.8) & (pl.col("distance_km") < 5.8)).then(pl.lit("3-5km"))
            .when((pl.col("distance_km") >= 5.8) & (pl.col("distance_km") < 10)).then(pl.lit("6-10km"))
            .otherwise(pl.lit(">10km")).alias("distance_range")
        )
        .group_by(["dt_interval", "distance_range"])
        .agg(pl.count("activityType.typeKey").alias("activity_count"))
    )

    # Create a stacked bar chart
    chart_activity_distances = (
        alt.Chart(_activity_counts.collect())
        .mark_bar(width=bar_width)
        .encode(
            x=alt.X("dt_interval:T", title="Tid", scale=alt.Scale(domain=[to_alt_dt(start_date), to_alt_dt(end_date)])),
            y=alt.Y("activity_count:Q", title="Antal aktiviteter"),
            color=alt.Color('distance_range:N', scale=alt.Scale(domain=list(_colors.keys()), range=list(_colors.values()))),
            tooltip=["dt_interval:O", "distance_range:N", "activity_count:Q"]
        )
        .properties(
            title=f"Antal aktiviteter med distanser per {month_text}",
            width=ALTAIR_WIDTH,
            height=200
        )
    )

    chart_activity_distances.interactive()
    return


@app.cell(hide_code=True)
def count_gym_vs_running(
    ALTAIR_WIDTH,
    alt,
    bar_width,
    current_garmin_data,
    end_date,
    interval_input,
    jefit_df,
    month_text,
    pl,
    start_date,
    to_alt_dt,
):
    gym_count_agg_df = (
        jefit_df.with_columns(pl.col('dt').dt.truncate('1d').alias('1d_interval'))
            .group_by(pl.col('1d_interval'))
            .agg()
        .with_columns(pl.col('1d_interval').dt.truncate(interval_input).alias('dt_interval'))
        .group_by('dt_interval').agg(pl.col('dt_interval').len().alias('count')).filter(pl.col('dt_interval').is_between(start_date, end_date))
        .with_columns(pl.lit('gym').alias('category'))
    )

    running_count_agg_df = (
        current_garmin_data.filter(pl.col('activityType.typeKey') == 'running').with_columns(pl.col('dt').dt.truncate('1d').alias('1d_interval'))
            .group_by(pl.col('1d_interval'))
            .agg()
        .with_columns(pl.col('1d_interval').dt.truncate(interval_input).alias('dt_interval'))
        .group_by('dt_interval').agg(pl.col('dt_interval').len().alias('count')).filter(pl.col('dt_interval').is_between(start_date, end_date))
        .with_columns(pl.lit('running').alias('category'), pl.col('dt_interval').dt.date().alias('dt_interval'))
    )

    _joined_agg_df = pl.concat((gym_count_agg_df, running_count_agg_df)).collect()

    chart_count_activities = (
        alt.Chart(_joined_agg_df).mark_bar(width=bar_width)
        .encode(
            x=alt.X("dt_interval:T", title="Tid", scale=alt.Scale(domain=[to_alt_dt(start_date), to_alt_dt(end_date)])), 
            y=alt.Y('count', title='Antal'),
            color=alt.Color('category', title='Aktivitet', scale=alt.Scale(range=('green', 'blue')))
        )
    ).properties(
            title=f"Antal gånger aktiviteter utförts per {month_text}",
            width=ALTAIR_WIDTH,
            height=200
        )

    chart_count_activities
    return


@app.cell(hide_code=True)
def get_walk_run_distance_chart(
    ALTAIR_WIDTH,
    alt,
    bar_width,
    end_date,
    month_text,
    start_date,
    to_alt_dt,
    walk_run_df,
):
    distance_chart = alt.Chart(walk_run_df.collect()).mark_bar(width=bar_width).encode(x=alt.X('dt_interval', title='Datum', scale=alt.Scale(domain=[to_alt_dt(start_date), to_alt_dt(end_date)])), y=alt.Y('value', title='kilometer'), color=alt.Color('type', title='Kategori')).properties(
        title=f'Antal km per {month_text}',
        width=ALTAIR_WIDTH,
        height=300
    )
    distance_chart
    return


@app.cell(hide_code=True)
def chart_count_of_distances(activity_input, alt, current_garmin_data, pl):
    df_distance_in_km = current_garmin_data.filter(pl.col('activityType.typeKey').eq(activity_input)).select('dt', (pl.col('distance')/1000).alias('distance_km'))

    df_distance_in_km_rounded = df_distance_in_km.with_columns(pl.col('distance_km').round())
    df_grouped_ = df_distance_in_km_rounded.group_by(pl.col('distance_km')).agg(pl.len().alias('count')).sort(by='distance_km').collect()

    alt.Chart(df_grouped_).mark_bar(size=20).encode(x=alt.X('distance_km:N', title='Kilometer'), y=alt.Y('count:Q', title='Antal')).properties(height=200, title='Antal löprundor grupperade på längd i kilometer')
    return


@app.cell(hide_code=True)
def get_records_tempo(df_activity_tempo, is_wasm, mo, pl):
    def float_to_minutes_seconds(minutes_float):
        # Extract minutes
        minutes = int(minutes_float)
        # Extract seconds
        seconds = int((minutes_float - minutes) * 60)
        # Format as minutes:seconds
        return f"{minutes}:{seconds:02}"

    _df_with_times = df_activity_tempo.filter(pl.col('distance').is_between(5800, 6200)).with_columns(time=pl.col('mins_per_km').map_elements(float_to_minutes_seconds, return_dtype=pl.String).alias('tempo')).sort(by='mins_per_km', descending=False).select('dt', 'time').rename({'dt': 'Datum', 'time': 'Tempo (min/km)'}).collect()

    if not is_wasm():
        _table = mo.ui.table(_df_with_times, page_size=5, show_column_summaries=False)
    else:
        _table = mo.plain(_df_with_times)

    mo.output.append(mo.md('## Rekord hastighet för 6 km'))
    mo.output.append(_table)
    return


@app.cell(hide_code=True)
def get_records_distance(current_garmin_data, is_wasm, mo, pl):
    _longest_activities_df = current_garmin_data.select('dt', 'distance', 'activityType.typeKey', (pl.col('distance') / 1000).round().alias('km')).sort(by='distance', descending=True).rename({'activityType.typeKey': 'Aktivitet', 'dt': 'Datum'}).select('Datum', 'km', 'Aktivitet').collect()

    mo.output.append(mo.md('## Rekord distanser for period'))

    if not is_wasm():
        _t = mo.ui.table(_longest_activities_df, show_column_summaries=False)
    else:
        _t = mo.plain(_longest_activities_df)

    mo.output.append(_t)
    return


@app.cell(hide_code=True)
def explore_garmin_dataset(current_garmin_data, is_wasm, mo):
    _df = current_garmin_data.collect()
    if is_wasm():
        mo.plain(_df)
    else:
        mo.ui.dataframe(_df, page_size=10)
    return


@app.cell(hide_code=True)
def get_walking_distance_from_apple_df(apple_df, interval_input, mo, pl):
    mo.stop(apple_df.collect().is_empty() or not interval_input)

    walk_distance_df = apple_df.filter(pl.col('metric') == 'distancewalkingrunning').group_by(['dt', 'metric']).agg(pl.col('value').sum())

    walk_distance_df = walk_distance_df.with_columns(pl.col('dt').dt.truncate(every=interval_input).alias('dt_interval'), pl.lit('walk').alias('type')).select('dt_interval', 'value', 'type')
    return (walk_distance_df,)


@app.cell(hide_code=True)
def get_date_range_from_form(form, mo):
    mo.stop(form.value is None, mo.md('Fyll i data'))
    interval_input = form.value['interval_input']
    start_date, end_date = form.value['date_range']
    return end_date, interval_input, start_date


@app.cell(hide_code=True)
def get_run_distance_df_from_garmin(
    current_garmin_data,
    interval_input,
    mo,
    pl,
):
    mo.stop(current_garmin_data.collect().is_empty() or not interval_input)

    run_distance_df = current_garmin_data.filter(pl.col('activityType.typeKey') == 'running').with_columns(pl.col('dt').dt.truncate(every=interval_input).alias('dt_')).with_columns(pl.col('dt_').dt.date().alias('dt_interval')).select('dt_interval', 'distance').group_by('dt_interval').agg(pl.col('distance').sum() / 1000).rename({'distance': 'value'}).with_columns(pl.lit('run').alias('type'))
    return (run_distance_df,)


@app.cell(hide_code=True)
def get_concatenade_run_walk_df(
    end_date,
    pl,
    run_distance_df,
    start_date,
    walk_distance_df,
):
    walk_run_df = pl.concat([walk_distance_df, run_distance_df]).sort(by='dt_interval').filter(pl.col('dt_interval').is_between(start_date, end_date))
    return (walk_run_df,)


@app.cell(hide_code=True)
def create_logger():
    import logging

    # Create a logger object
    logger = logging.getLogger('health')
    logger.setLevel(logging.DEBUG)

    # Create a console handler
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)

    # Create a formatter that outputs time in HH:MM
    formatter = logging.Formatter('%(asctime)s - %(message)s', datefmt='%H:%M')
    ch.setFormatter(formatter)

    # Add the handler to the logger
    logger.addHandler(ch)
    return (logger,)


@app.cell(hide_code=True)
def get_median_pulse_zones_chart(
    ALTAIR_WIDTH,
    activity_type_form,
    alt,
    bar_width,
    current_garmin_data,
    end_date,
    interval_input,
    mo,
    month_text,
    pl,
    start_date,
    to_alt_dt,
):
    mo.stop(any(_ is None for _ in activity_type_form.value.values()) is True, mo.md('Välj aktivitet för zoner'))

    activity_input = activity_type_form.value['activity_input']

    interval_median_zones = (current_garmin_data.filter(pl.col('activityType.typeKey').eq(activity_input))
        .with_columns([
            pl.col("dt").dt.truncate(interval_input).alias("dt_interval"),
            (pl.col("secsInZone5") / 60),
            (pl.col("secsInZone4") / 60),
            (pl.col("secsInZone3") / 60),
            (pl.col("secsInZone2") / 60),
            (pl.col("secsInZone1") / 60),
        ])
        .group_by("dt_interval")
        .agg([
            pl.col("secsInZone1").median().alias("median_zone1"),
            pl.col("secsInZone2").median().alias("median_zone2"),
            pl.col("secsInZone3").median().alias("median_zone3"),
            pl.col("secsInZone4").median().alias("median_zone4"),
            pl.col("secsInZone5").median().alias("median_zone5"),
        ])
    ).filter(pl.col('dt_interval').is_between(start_date, end_date))

    _colors = {'median_zone1': 'gray', 'median_zone2': 'lightblue', 'median_zone3': 'green', 'median_zone4': 'orange', 'median_zone5': 'red'}

    interval_median_zones_chart = alt.Chart(interval_median_zones.collect()).transform_fold(
        ["median_zone1", "median_zone2", "median_zone3", "median_zone4", "median_zone5"],
        as_=['zone', 'median_time']
    ).mark_bar(size=bar_width).encode(
        x=alt.X('yearmonth(dt_interval):T', title='Månad', scale=alt.Scale(domain=[to_alt_dt(start_date), to_alt_dt(end_date)])),
        y=alt.Y('median_time:Q', title='Median tid i minuter'),
        color=alt.Color('zone:N', scale=alt.Scale(
            domain=list(_colors.keys()), range=list(_colors.values())), 
            title='Heart Rate Zones'),
        tooltip=['dt_interval:T', 'zone:N', 'median_time:Q'],
        order=alt.Order('zone:N', sort='ascending')
    ).properties(
        title=f'Median tid per aktivitet & tider i puls zoner för {month_text}',
        width=ALTAIR_WIDTH,
        height=300,
    )
    return activity_input, interval_median_zones_chart


@app.cell(hide_code=True)
def get_df_for_median_tempo(
    activity_input,
    activity_type_form,
    current_garmin_data,
    interval_input,
    mo,
    pl,
):
    mo.stop(any(_ is None for _ in activity_type_form.value.values()) is True, mo.md('Välj aktivitet för zoner'))
    # mo.stop(activity_for_zones.value is None, mo.md('Välj aktivitet för zoner'))

    df_activity_tempo = current_garmin_data.filter(pl.col('activityType.typeKey').eq(activity_input)).select('dt', 'distance', ((pl.col('duration')/60)/(pl.col('distance')/1000)).alias('mins_per_km'))

    chart_data_mins_per_km = df_activity_tempo.with_columns(pl.col('dt').dt.truncate(interval_input).alias('dt_interval')).group_by('dt_interval').agg(pl.col('mins_per_km').median().alias('mean_mins_per_km'))
    return chart_data_mins_per_km, df_activity_tempo


@app.cell(hide_code=True)
def _(current_garmin_data, pl):
    fastest_6km_runs = current_garmin_data.filter((pl.col('activityType.typeKey') == 'running') & (pl.col('distance').is_between(5800, 6200))).select((pl.col('duration') / 60).round().alias('Minuter'), pl.col('dt').dt.date().alias('datum')).sort(by='Minuter', descending=False).limit(10).collect()

    longest_running_distances = current_garmin_data.filter(pl.col('activityType.typeKey') == 'running').sort(by='distance', descending=True).select((pl.col('distance') / 1000).round(1).alias('km'), pl.duration(seconds=pl.col('duration')).alias('tid'), pl.col('dt').dt.date().alias('datum')).limit(10).collect()
    return fastest_6km_runs, longest_running_distances


@app.cell(hide_code=True)
def display_gym_records(
    end_date,
    fastest_6km_runs,
    longest_running_distances,
    mo,
    start_date,
):
    mo.output.append(mo.md(f'### Längsta löpningen {start_date.year} - {end_date.year} 🥇'))
    mo.output.append(mo.plain(longest_running_distances))
    mo.output.append(mo.md(f'### Snabbast 6km löpningen {start_date.year} - {end_date.year} 🥇'))
    if not fastest_6km_runs.is_empty():
        mo.output.append(mo.plain(fastest_6km_runs))
    else:
        mo.output.append(mo.md('Ingen'))
    return


@app.cell
def _():
    return


@app.cell(column=1, hide_code=True)
def _(mo):
    mo.md(r"""
    ## Generell hälso information
    """)
    return


@app.cell(hide_code=True)
def display_blood_pressure_chart(
    ALTAIR_WIDTH,
    alt,
    blood_pressure_agg,
    end_date,
    month_text,
    start_date,
    to_alt_dt,
):
    _df = blood_pressure_agg.collect() if hasattr(blood_pressure_agg, 'collect') else blood_pressure_agg
    _base = alt.Chart(_df).mark_line(interpolate="monotone").encode(
        x=alt.X('dt_interval:T', title='Datum', scale=alt.Scale(domain=[to_alt_dt(start_date), to_alt_dt(end_date)])),
    ).properties(
        title=f'Medel blodtryck per {month_text} (snitt)',
        width=ALTAIR_WIDTH,
        height=300,
    )

    _dia = _base.encode(y=alt.Y('bloodpressurediastolic:Q', title='Dia', scale=alt.Scale(domainMin=50)), color=alt.value('lightblue'))
    _sys = _base.encode(y=alt.Y('bloodpressuresystolic:Q', title='Sys'), color=alt.value('red'))

    _base + _dia + _sys
    return


@app.cell(hide_code=True)
def display_weight_fat_plot(
    ALTAIR_WIDTH,
    alt,
    end_date,
    mo,
    month_text,
    pl,
    start_date,
    to_alt_dt,
    weight_fat_df,
):
    mo.stop(None in [start_date, end_date])

    mo.output.append(mo.md(f'##Vikt och fett% snitt per {month_text}'))

    _df_weight = weight_fat_df.filter((pl.col('bodymass') >= 70), (pl.col('dt_interval').is_between(start_date, end_date))).with_columns(pl.col('bodymass').rolling_mean(window_size=3, center=True).fill_null(strategy='mean').alias('weight'))

    df_min_weight = _df_weight['bodymass'].min()
    df_max_weight = _df_weight['bodymass'].max()

    _base = alt.Chart(_df_weight).properties(width=ALTAIR_WIDTH, height=300)

    _weight = _base.mark_line(strokeWidth=3, color='red', interpolate="monotone").encode(x=alt.X('dt_interval:T', title='Datum', scale=alt.Scale(domain=[to_alt_dt(start_date), to_alt_dt(end_date)])), y=alt.Y('weight:Q', title='Vikt (kg)   🟥', scale=alt.Scale(domainMin=df_min_weight, domainMax=df_max_weight)))

    _df_fat = weight_fat_df.filter(pl.col('dt_interval').is_between(start_date, end_date)).with_columns((pl.col('bodyfatpercentage') * 100).rolling_mean(window_size=3, center=True).fill_null(strategy='mean').alias('fat'))

    df_min_fat = _df_fat['bodyfatpercentage'].min() * 100
    df_max_fat = _df_fat['bodyfatpercentage'].max() * 100

    _fat = alt.Chart(_df_fat).mark_line(strokeWidth=3, interpolate="monotone", color='gray').encode(x=alt.X('dt_interval:T', scale=alt.Scale(domain=[to_alt_dt(start_date), to_alt_dt(end_date)])), y=alt.Y('fat:Q', title='Fett%   ◻️', scale=alt.Scale(domainMin=df_min_fat, domainMax=df_max_fat)))

    # 2nd axis added
    _chart = alt.layer(_weight, _fat).resolve_scale(y='independent')
    mo.output.append(_chart)
    return


@app.cell(hide_code=True)
def display_blood_pressure_records(
    blood_pressure_agg,
    end_date,
    mo,
    month_text,
    pl,
    start_date,
):
    mo.output.append(mo.md(f'### Högsta mätningar av blodtryck {start_date.year} - {end_date.year} (snitt per {month_text})🩸'))
    mo.output.append(mo.plain(blood_pressure_agg.select(pl.col('dt_interval').alias('datum'), pl.col('bloodpressuresystolic').round(1), pl.col('bloodpressurediastolic').round(1)).sort(by='bloodpressurediastolic', descending=True).limit(5)))

    mo.output.append(mo.md(f'### Lägsta mätningar av blodtryck {start_date.year} - {end_date.year} (snitt per {month_text}) 🩸'))
    mo.output.append(mo.plain(blood_pressure_agg.select(pl.col('dt_interval').alias('datum'), pl.col('bloodpressuresystolic').round(1), pl.col('bloodpressurediastolic').round(1)).sort(by='bloodpressurediastolic', descending=False).limit(5)))

    mo.output.append(mo.md('_**Tips:** För mer exakta värden ändra till `gruppera per = dag` och en snävare `period`_'))
    return


@app.cell(hide_code=True)
def explore_blood_pressure_df(
    blood_pressure_data_grouped,
    interval_input,
    mo,
    month_text,
    pl,
):
    # mo.stop(any(_ is None for _ in activity_type_form.value.values()) is True)
    _df_grouped = blood_pressure_data_grouped.collect()
    mo.stop(interval_input is None or _df_grouped.height == 0, mo.md('Ingen data för period'))

    mo.output.append(mo.md(f'### Utforska blodtrycket för perioden med snitt per {month_text}'))

    blood_pressure_exploded = _df_grouped.pivot('metric', index="dt", values="value")

    blood_pressure_agg = blood_pressure_exploded.lazy().with_columns(pl.col('dt').dt.truncate(every=interval_input).alias('dt_interval')).group_by('dt_interval').agg(pl.mean(['bloodpressuresystolic', 'bloodpressurediastolic']).round(0)).sort(by='dt_interval').collect()

    mo.output.append(blood_pressure_agg)
    return (blood_pressure_agg,)


@app.cell
def _(mo):
    apple_run_button = mo.ui.run_button(label='Visa historik från Apple (blodtryck etc)', kind='success', tooltip='Detta kan ta lite tid ...')
    apple_run_button
    return (apple_run_button,)


@app.cell(hide_code=True)
async def load_apple_df(
    apple_file,
    apple_run_button,
    end_date,
    file_exists,
    is_mobile,
    is_wasm,
    logger,
    mo,
    pl,
    read_df,
    relevant_apple_colums,
    start_date,
):
    mo.stop(not apple_run_button.value, mo.md('Klicka på "Visa historik från Apple"'))

    if file_exists(apple_file):
        if is_wasm():
            if is_mobile():
                _m = 'Du kör från en mobil med begränsat minne, kör Chrome från dator.'
                logger.warning(_m)
                mo.stop(True, 'Du kör från en mobil med begränsat minne, kör Chrome från dator.')
            else:
                logger.info('You are running WASM from a computer browser')
            all_apple_df = await read_df(apple_file)
        else:
            all_apple_df = pl.scan_parquet(apple_file)

        _df = all_apple_df.filter(pl.col('dt').is_between(start_date, end_date))

        # Collect only necessary stats for display
        _stats = all_apple_df.select(
            min_dt=pl.col('dt').min(),
            max_dt=pl.col('dt').max(),
            count=pl.len()
        ).collect()

        _min_dt = _stats.get_column('min_dt')[0]
        _max_dt = _stats.get_column('max_dt')[0]
        _count = _stats.get_column('count')[0]

        mo.output.append(mo.Html(f'''<u><b>Apple Hälsa data</b/></u>Period: {_min_dt:%Y-%m-%d} - {_max_dt:%Y-%m-%d} [{_count} rader]</br>'''))
    else:
        _df = pl.DataFrame({k: [] for k in relevant_apple_colums}).lazy()
        all_apple_df = _df

    apple_df = _df
    return all_apple_df, apple_df


@app.cell(hide_code=True)
def display_apple_df(apple_df, is_wasm, mo):
    # apple_df = pl.DataFrame(apple_data).with_columns(dt=pl.col('date').str.to_date())
    mo.output.append(mo.md(f'## Utforska all Apple Hälsa data'))
    _df = apple_df.collect()
    if not is_wasm():
        mo.output.append(mo.ui.dataframe(_df))
    else:
        mo.output.append(mo.plain(_df))
    return


@app.cell(hide_code=True)
def get_weight_fat_df_from_apple_df(apple_df, interval_input, mo, pl):
    # Use collect() before pivot as pivot is not fully lazy in all contexts and apple_df is filtered here
    _df_filtered = apple_df.filter(pl.col('metric').is_in(['bodymass', 'bodyfatpercentage'])).collect()
    mo.stop(_df_filtered.height == 0 or not interval_input)

    weight_fat_df = _df_filtered.pivot('metric', index='dt', values='value', aggregate_function='mean').lazy().with_columns(pl.col('dt').dt.truncate(every=interval_input).alias('dt_interval')).group_by('dt_interval').agg(pl.mean(['bodymass', 'bodyfatpercentage'])).collect()
    return (weight_fat_df,)


@app.cell(hide_code=True)
def display_apple_health_metrics():
    # apple_df.select('metric').unique()
    return


@app.cell(hide_code=True)
def group_blood_pressure_exclude_duplicates(
    apple_df,
    end_date,
    pl,
    start_date,
):
    blood_pressure_data_grouped = apple_df.select(['dt', 'metric', 'value']).filter(pl.col('metric').is_in(['bloodpressuresystolic', 'bloodpressurediastolic']), pl.col('dt').is_between(start_date, end_date)).group_by(['dt', 'metric']).agg(pl.mean('value')).sort(by='dt')
    return (blood_pressure_data_grouped,)


@app.cell(column=2, hide_code=True)
def _(mo):
    mo.md(r"""
    ## Gym övningar
    """)
    return


@app.cell
def _(mo):
    gym_run_button = mo.ui.run_button(label='Visa gym historik', kind='success', tooltip='Detta kan ta lite tid...')
    gym_run_button
    return (gym_run_button,)


@app.cell(hide_code=True)
async def read_jefit_df(
    file_exists,
    gym_run_button,
    jefit_file,
    mo,
    pl,
    read_df,
):
    mo.stop(not gym_run_button.value, mo.md('Klicka på "Visa gym historik"'))

    # Create empty and attempt load
    jefit_df = pl.DataFrame({'dt': [], 'excercise': [], 'rep_max': [], 'sets': []}, schema={'dt': pl.datatypes.Datetime, 'excercise': pl.datatypes.String, 'rep_max': pl.datatypes.Float32, 'sets': pl.datatypes.List}).lazy()

    if file_exists(jefit_file):
        jefit_df = await read_df(jefit_file)
        if jefit_df is not None:
            jefit_df = jefit_df.sort(by='dt').select('dt', 'excercise', pl.col('rep_max').cast(pl.Float32), 'sets').with_columns(rep_max=pl.col('rep_max').round(0))
        else:
            mo.stop(True, mo.md('Unable to load Jefit Gym data'))
    return (jefit_df,)


@app.cell(hide_code=True)
def present_count_of_gym_excercises(jefit_df, mo):
    mo.output.append(mo.md('### Antal övningar över hela perioden'))
    mo.output.append(jefit_df.group_by('excercise').len().sort(by='len', descending=True).rename({'excercise': 'Övning', 'len': 'Antal'}).collect())
    return


@app.cell(hide_code=True)
def define_excercises_for_graphs():
    # Exercises to include in charts

    exercises = [
        'Barbell Bench Press',
        'Barbell Squat',
        'Barbell Deadlift',
        'Barbell Preacher Curl'
    ]
    return (exercises,)


@app.cell(hide_code=True)
def gym_1rm_wide_form(jefit_df, pl):
    # Wide form (not optimal for Altair)

    jefit_rep_max_df = jefit_df.collect().pivot(on='excercise', index='dt', values='rep_max', aggregate_function='max')

    jefit_max_excercise_df = jefit_rep_max_df.lazy().group_by_dynamic('dt', every='1mo').agg(pl.exclude('dt').max()).collect()
    jefit_max_excercise_df
    return


@app.cell(hide_code=True)
def get_1rm_gym_for_all_months(exercises, jefit_df, mo, pl):
    jefit_max_rep_df = (
        jefit_df.select(pl.col('dt')
            .dt.truncate('1mo'), 'excercise', 'rep_max')
            .group_by('dt', 'excercise')
            .agg(pl.col('rep_max').max().round(0))
            .sort(by='dt')
            .filter(pl.col('excercise').is_in(exercises))
    )

    mo.output.append(mo.md('### Övningar 1RM'))
    _df = jefit_max_rep_df.collect()
    mo.output.append(_df)
    return (jefit_max_rep_df,)


@app.cell(hide_code=True)
def get_bar_chart_1rm_gym_per_month(alt, exercises, jefit_max_rep_df, mo):
    _chart = alt.Chart(jefit_max_rep_df.collect()).mark_bar(size=5).encode(
        column=alt.Column('yearmonth(dt):O'),
        x=alt.X('excercise:N', title='Övning', sort=exercises),
        y=alt.Y('rep_max:Q').scale(zero=True, domain=[50, 100]),
        color=alt.Color('excercise:N')
    ).properties(
        width=60,
        height=120
    )

    mo.output.append(mo.md('### Gym övningar 1RM per månad'))
    mo.output.append(_chart)
    return


@app.cell(hide_code=True)
def present_selectable_1rm_line_chart(
    ALTAIR_WIDTH,
    alt,
    jefit_max_rep_df,
    mo,
    pl,
):
    # This defines a mouseover selection for points. fields=["dt"] allows Altair to identify other points with the same date. You will use this to create a vertical line highlight when a user hovers over a point.
    _hover = alt.selection_point(
        fields=["dt"],
        nearest=True,
        on="mouseover",
        empty="none",
    )

    _jefit_collected = jefit_max_rep_df.collect()

    _benchpress_max_rep = _jefit_collected.filter(pl.col('excercise') == 'Barbell Bench Press')['rep_max'].max()
    _benchpress_max_rep = _benchpress_max_rep if _benchpress_max_rep is not None else 0

    _curl_max_rep = _jefit_collected.filter(pl.col('excercise') == 'Barbell Preacher Curl')['rep_max'].max()
    _curl_max_rep = _curl_max_rep if _curl_max_rep is not None else 0

    _max_benchpress = alt.Chart().mark_rule(color='red', strokeDash=[5, 5]).encode(
        y=alt.datum(_benchpress_max_rep),
        size=alt.value(1),
    )

    _max_curl = alt.Chart().mark_rule(color='orange', strokeDash=[5, 5]).encode(
        y=alt.datum(_curl_max_rep),
        size=alt.value(1),
    )

    # Add selection feature and add as params
    brush = alt.selection_interval(encodings=["x"])

    jefit_chart = alt.Chart(_jefit_collected).mark_line(size=3, interpolate="monotone").encode(
        x=alt.X('yearmonth(dt):T'),
        y=alt.Y('rep_max:Q').scale(domainMin=35),
        color=alt.Color('excercise:N', title='Övning', scale=alt.Scale(range=['red', 'white', 'orange', 'yellow'])),
        tooltip=[
                alt.Tooltip("dt", title="Datum"),
                alt.Tooltip("rep_max", title="1RM"),
                alt.Tooltip("excercise", title="Övning"),
            ]
    ).add_params(_hover, brush).properties(width=ALTAIR_WIDTH)


    # Use Marimo to select from ti
    selection = mo.ui.altair_chart(jefit_chart + _max_benchpress + _max_curl, chart_selection=False, legend_selection=False)

    mo.output.append(mo.md('### 1RM (max vikt för 1 rep) för gym övningar'))
    mo.output.append(mo.md('Välj period för mer detaljer'))
    mo.output.append(selection)
    return (selection,)


@app.cell(hide_code=True)
def display_detailed_table_selected_1rm_period(
    datetime,
    jefit_df,
    mo,
    pl,
    selection,
):
    jefit_start_ts = jefit_end_ts = 0

    def ts_to_iso(ts: int):
        return datetime.datetime.fromtimestamp(ts / 1000)

    # Ugly way of extracting TS as "layered" does not currently work as expected in Marimo
    try:
        jefit_start_ts, jefit_end_ts = list(list(selection.selections.values()).pop().values()).pop()
    except IndexError:
        mo.output.append(mo.md('Välj från gym listan för resultat'))
    else:
        mo.output.append(mo.md(f'**Gym period vald:** {ts_to_iso(jefit_start_ts):%Y-%m-%d} - {ts_to_iso(jefit_end_ts):%Y-%m-%d}'))

    mo.stop(jefit_start_ts == 0, mo.md('Markera en period i gym grafen för mer detaljer'))
    mo.output.append(mo.md('### Vald gym period'))
    mo.output.append(jefit_df.filter(pl.col('dt').is_between(ts_to_iso(jefit_start_ts), ts_to_iso(jefit_end_ts))).collect())
    return


@app.cell(hide_code=True)
def display_gym_records_list(jefit_df, mo, pl):
    for e in ('Barbell Bench Press', 'Barbell Squat'):
        mo.output.append(mo.md(f'### 💪🏻🎖️ Max vikt för {e}'))
        max_excercise_rep= jefit_df.with_columns(rep_max=pl.col('rep_max').round(0)).filter(pl.col('excercise') == e).sort(by='rep_max', descending=True).limit(n=5).collect()
        mo.output.append(max_excercise_rep)
    return


@app.cell
def _():
    return


@app.cell(column=3, hide_code=True)
def start_garmin_download(is_wasm, mo):
    mo.stop(is_wasm() is True, mo.md("Inaktiverar Garmin Connect inladdning när vi kör WASM"))
    mo.output.append(mo.md('## Ladda in Garmin aktiviteter från API'))
    input_run_garmin_import = mo.ui.run_button(label='Starta')
    mo.output.append(input_run_garmin_import)
    return (input_run_garmin_import,)


@app.cell(hide_code=True)
def get_garmin_credentials(dotenv, is_wasm, mo, os):
    mo.stop(is_wasm() is True, mo.md("Inaktiverar Garmin Connect inladdning när vi kör WASM"))

    if not is_wasm():
        dotenv.load_dotenv('.env')

    _username = os.getenv('GARMIN_USERNAME', None)
    _password = os.getenv('GARMIN_PASSWORD', None)

    garmin_login_found = None

    if None in (_username, _password):
        _u = mo.ui.text()
        _p = mo.ui.text(kind='password')
        garmin_login_form = mo.md("""### Fyll i Garmin uppgifter
        Du kan även skapa en `.env` fil med följande `GARMIN_USERNAME` samt `GARMIN_PASSWORD`

        Användarnamn: {username}

        Lösenord: {password}
        """).batch(username=_u, password=_p).form()
        mo.output.append(garmin_login_form)
    else:
        garmin_login_found = (_username, _password)
    return (garmin_login_found,)


@app.cell(hide_code=True)
def get_garmin_raw_data(
    Garmin,
    cache,
    datetime,
    garmin_login_found,
    input_run_garmin_import,
    is_wasm,
    logger,
    mo,
):
    mo.stop(not garmin_login_found, mo.md('Avaktar med att hämta Garmin data'))
    mo.stop(not input_run_garmin_import.value, mo.md('Starta importering av Garmin'))

    if not is_wasm():
        # Workaround ensure not attempt to micropip this while used as WASM
        exec('''from persist_cache import cache
    from garminconnect import Garmin
        ''')

    DateLike = str | datetime.date

    try:
        garmin_username, garmin_password
    except NameError:
        garmin_username, garmin_password = garmin_login_found

    logger.debug('Logging in to Garmin')
    gc = Garmin(garmin_username, garmin_password)
    gc_login = gc.login()

    @cache
    def cached_get_garmin_activites(dt: DateLike):
        variable = gc.get_activities_fordate(fordate=dt)
        logger.debug(f'Downloading Garmin activity data for {dt}')
        return variable

    @cache
    def cached_get_garmin_heart_rates(activityId: int) -> dict:
        """Return {'secsInZone1': 70, ...}"""
        logger.debug(f'Downloading Garmin heart rate data for {activityId}')
        hr_zones = gc.get_activity_hr_in_timezones(activityId)
        zone_columns = {}
        for z in hr_zones:
            n = z['zoneNumber']
            # logger.debug(f"{n} {z['zoneLowBoundary']}")
            zone_columns[f'secsInZone{n}'] = z['secsInZone']
        logger.debug(zone_columns)
        return zone_columns


    def get_raw_garmin_data(dt: DateLike | list[DateLike], pb=None) -> dict:
        all_activities = []

        if isinstance(dt, str):
            dt = datetime.date.fromisoformat(dt)

        if not isinstance(dt, list):
            dt = [dt]

        with mo.status.progress_bar(total=len(dt)) as bar:
            for dt_ in dt:
                if isinstance(dt_, str):
                    dt_ = datetime.date.fromisoformat(dt_)
                if dt_ == datetime.date.today():
                    logger.debug(f'Skipping get data for today (possibly incomplete) {dt_}')
                    continue
                if dt_ > datetime.date.today():
                    logger.debug(f'Skipping future date {dt_}')
                    continue
                if isinstance(dt_, datetime.date):
                    dt_ = dt_.isoformat()
                activities = cached_get_garmin_activites(dt_)

                # Adding another API call to get heart rate zones
                for activity in activities.get('ActivitiesForDay', {}).get('payload', []):
                    zones = cached_get_garmin_heart_rates(activity['activityId'])
                    for field, value in zones.items():
                        activity[field] = value

                all_activities.append(activities)
                bar.update(subtitle=str(dt_))
        return all_activities


    def get_garmin_profile() -> dict:
        return gc.get_user_profile()


    # get_raw_garmin_data(username=username, password=password, dt=['2025-04-12', '2025-04-14'])
    dob_ = get_garmin_profile()['userData']['birthDate']
    dob = datetime.date.fromisoformat(dob_).year
    return dob, garmin_password, garmin_username, gc, get_raw_garmin_data


@app.cell(hide_code=True)
async def get_garmin_df_and_filter(
    datetime,
    dob,
    file_exists,
    garmin_file,
    get_first_garmin_activity,
    get_raw_garmin_data,
    input_run_garmin_import,
    logger,
    mo,
    pl,
    read_df,
):
    garmin_activities =  pl.DataFrame()

    displayed_years = set()

    def remove_existing_dates(current_list: list[str], remove_dates: set[datetime.date]) -> list[str]:
        logger.info(f'Initial list of dates size: {len(current_list)}')
        output_list = []
        for d in current_list:
            _date = datetime.date.fromisoformat(d)
            if _date in remove_dates:
                continue
            else:
                output_list.append(d)
        logger.info(f'Output list of dates size: {len(output_list)}')
        return output_list


    def get_pulse_zones(for_year: int=datetime.date.today().year, birth_year: int=1977) -> list[list[int, int]]:
        """Returns a list of heart-rates ranges for zones 0-5"""
        age = for_year - birth_year
        zone_percs = (
            [0.0, 0.5],
            [0.5, 0.6],
            [0.6, 0.7],
            [0.7, 0.8],
            [0.8, 0.9],
            [0.9, 1.0]
        )
        max_rate = 220 - age
        zone_rates = []
        global displayed_years
        if for_year not in displayed_years:
            mo.output.append(mo.md(f'### <u>Puls zoner för ålder {age}</u>'))
        for zone, (start, end) in enumerate(zone_percs):
            start_ = round(start * max_rate)
            end_ = round(end * max_rate)
            if for_year not in displayed_years:
                mo.output.append(mo.md(f'**Puls Zon {zone}:** {start_}-{end_}'))
            zone_rates.append((start_, end_))
        displayed_years.add(for_year)
        return zone_rates


    def raw_heart_rate_to_zones(start_iso, duration_secs, heart_rate_values: list[list], hr_start: int, hr_end: int) -> int:
        """Based on list of heart rates get count of minutes - REPLACED BY OTHER FUNCTION"""
        # logger.debug(f'Processing activity for {start_iso} for range {hr_start}-{hr_end}')
        ts_start = int(datetime.datetime.fromisoformat(start_iso).timestamp() * 1000)
        ts_end = ts_start + int(duration_secs * 1000)
        mins = 0
        previous_timestamp = None
        heart_rate_values.pop(0)
        for timestamp, heart_rate in heart_rate_values:
            # logger.debug(f'heart rate: {heart_rate}')
            # print(f'{ts_start=} {timestamp=} {ts_end}')
            if previous_timestamp is not None and heart_rate is not None:
                if not ts_start <= timestamp < ts_end:
                    previous_timestamp = timestamp
                    continue
                delta_time = (timestamp - previous_timestamp) / 1000 
                if hr_start <= heart_rate < hr_end:
                    # logger.debug(f'{heart_rate} between {hr_start}-{hr_end}: Mins {mins}')
                    mins += int(delta_time)
            previous_timestamp = timestamp
        # logger.debug(f'------ Returning {mins} for {hr_start}-{hr_end} ------')
        return mins


    def get_hr_col(raw_heart_rates: dict, hr_start: int, hr_end: int, zone: int) -> list[pl.struct]:
        """Create Polars exrpession from row adding pulse zones"""
        return pl.struct(pl.all()).map_elements(lambda row: raw_heart_rate_to_zones(row['startTimeLocal'], row['duration'], raw_heart_rates, hr_start, hr_end), return_dtype=pl.Int64).alias(f'secsInHeartRateZone{zone}')


    def convert_garmin_activities(raw_garmin_data: dict, dob_: int) -> pl.DataFrame:
        """Main purpose normalizing to JSON and adding pulse zones"""
        output_df = pl.DataFrame()

        for a in raw_garmin_data:
            try:
                activities = pl.json_normalize(a['ActivitiesForDay']['payload'])
            except KeyError:
                # logger.debug(f'No ActivitiesForDay.payload key in payload: {a}')
                continue
            raw_heart_rates = a['AllDayHR']['payload']['heartRateValues']

            if raw_heart_rates:
                for_year = datetime.datetime.fromtimestamp(raw_heart_rates[0][0] / 1000).year
                pulse_zones = get_pulse_zones(for_year=2025, birth_year=dob_)
                hr_cols = [get_hr_col(raw_heart_rates, s, e, zone) for zone, (s, e) in enumerate(pulse_zones)]
            else:
                hr_cols = []

            df_with_hr = activities.with_columns(hr_cols)  # This is only working for past 3 months
            # df_with_zone_info = df_with_hr.with_columns(pl.col('activityId').map_elements(lambda a: cached_get_garmin_heart_rates(a)))

            if len(output_df) == 0:
                output_df = df_with_hr
            else:
                output_df = pl.concat([output_df, df_with_hr], how="diagonal")
        return output_df


    mo.stop(not input_run_garmin_import.value, mo.md('Starta Garmin import'))

    logger.info('Startar import')
    _start_date = get_first_garmin_activity()
    _end_date = datetime.date.today()
    date_range = [_start_date + datetime.timedelta(days=i) for i in range((_end_date - _start_date).days + 1)]
    date_range_iso = [date.isoformat() for date in date_range]

    existing_df = None

    if file_exists(garmin_file):
        # existing_df = pl.read_ndjson(_fn, schema_overrides={"dt": pl.Datetime})
        logger.info(f'Loading existing Garmin {garmin_file} and updating with existing')
        existing_df = await read_df(garmin_file)
        logger.info('Removing existing dates from the query to Garmin')
        existing_dates = set(existing_df.with_columns(date=pl.col('dt').dt.date())['date'])
        date_range_iso = remove_existing_dates(date_range_iso, existing_dates)

    _raw = get_raw_garmin_data(dt=date_range_iso)
    try:
        garmin_activities = convert_garmin_activities(_raw, dob_=dob).with_columns(        pl.col('startTimeLocal').str.strptime(pl.Datetime, format="%Y-%m-%dT%H:%M:%S%.f").alias('dt')
    )
    except pl.exceptions.ColumnNotFoundError:
        m = 'Empty dataframe - skipping'
        mo.stop(True, mo.md(m))
    # Move dt column to 1st and sort by it
    cols = garmin_activities.columns
    garmin_activities = garmin_activities.select(["dt"] + [col for col in cols if col != "dt"]).sort(by="dt").filter(pl.col('distance') > 1000)

    if file_exists(garmin_file):
        # Convert garmin_activities to lazy if existing_df is lazy
        all_garmin_data = pl.concat([existing_df, garmin_activities.lazy()], how='align').unique()
    else:
        logger.info('Building empty Garmin {garmin_file}')
        all_garmin_data = garmin_activities.lazy()
    all_garmin_data.collect().write_parquet(garmin_file)
    return


@app.cell(hide_code=True)
async def display_garmin_df_if_it_exists(
    file_exists,
    garmin_file,
    mo,
    read_df,
):
    mo.stop(file_exists(garmin_file) is False)

    _df = await read_df(garmin_file)
    mo.output.append(mo.md('### Data från Garmin'))
    mo.output.append(_df.collect())
    return


@app.cell(hide_code=True)
def get_first_garmin_activity(cache, datetime, gc, logger, mo):
    import time

    @cache
    def get_garmin_activity_by_offset(offset: int):
        time.sleep(1)
        g = gc.get_activities(limit=1, start=offset)
        return g


    def get_first_garmin_activity() -> datetime.datetime:
        first_garmin_activity = None
        with mo.status.spinner('Söker after första Garmin aktivitet') as _spinner:
            _start = 0
            while True:
                _activity_found = get_garmin_activity_by_offset(_start)
                if not _activity_found:
                    logger.info(f'Ingen aktivitet vid {_start}')
                    break
                _start += 100
                _spinner.update(f'Söker efter {_start}')
            while True:
                _activity_found = get_garmin_activity_by_offset(_start)
                if _activity_found:
                    _dt = _activity_found[0]['startTimeLocal']
                    logger.info(f'Last activity offset {_dt}')
                    first_garmin_activity = _dt
                    break
                _start -= 1
                _spinner.update(f'Söker efter {_start}')
        r = datetime.datetime.strptime(first_garmin_activity, '%Y-%m-%d %H:%M:%S').date()
        mo.output.append(mo.md(f'Hittade första datumet lagrad hos Garmin {r:%Y-%m-%d}'))
        return r

    return (get_first_garmin_activity,)


@app.cell(column=4, hide_code=True)
def upload_apple_health(is_wasm, mo):
    mo.stop(is_wasm() is True, mo.md("Inaktiverar Apple Hälsa inladdning när vi kör WASM"))
    mo.output.append(mo.md('## Ladda in Apple Hälsa data'))
    apple_health_upload = mo.ui.file().form()
    mo.output.append(apple_health_upload)
    return (apple_health_upload,)


@app.cell(hide_code=True)
async def process_apple_health_data(
    HealthData,
    NamedTemporaryFile,
    Path,
    ZipFile,
    apple_file,
    apple_health_upload,
    file_exists,
    logger,
    mo,
    pl,
    read_df,
):
    mo.stop(apple_health_upload.value is None, mo.md('Välj och ladda upp Apple Hälsa'))
    apple_health_content = apple_health_upload.value[0].contents

    def open_apple_health_zip(input_zip_data: bytes) -> list[dict]:
        activities = [
            "HKQuantityTypeIdentifierBloodPressureDiastolic",
            "HKQuantityTypeIdentifierBloodPressureSystolic",
            "HKQuantityTypeIdentifierBodyFatPercentage",
            "HKQuantityTypeIdentifierBodyMass",
            "HKQuantityTypeIdentifierBodyMassIndex",
            "HKQuantityTypeIdentifierDistanceWalkingRunning",
            "HKQuantityTypeIdentifierStepCount",
        ]
        activity_prefix = "HKQuantityTypeIdentifier"

        with NamedTemporaryFile('rb+') as input_zip:
            input_zip.write(input_zip_data)
            with ZipFile(input_zip.name) as myzip:
                logger.debug(myzip.namelist())
                with myzip.open("apple_health_export/export.xml") as myfile:
                    content = myfile.read()
                    with NamedTemporaryFile('rb+') as tmp_fp:
                        tmp_fp.write(content)
                        fn = tmp_fp.name
                        logger.debug(f"Temp apple health file file {fn}, size {Path(fn).stat().st_size}")
                        health_data = HealthData.read(fn)

        assert health_data is not None, 'If health data is None, something went wrong'
        input_records = health_data
        output = []
        for r in input_records.records:
            d = r.start.strftime("%Y-%m-%d")
            if r.name in activities:
                metric_name = r.name.replace(activity_prefix, "").lower()
                unit = r.unit
                value = r.value
                output.append({'metric': metric_name, 'unit': unit, 'value': value, 'date': d})
        return output

    with mo.status.spinner('Processar Apple hälsa data'):
        apple_data = open_apple_health_zip(apple_health_content)

    _df = pl.DataFrame(apple_data).with_columns(dt=pl.col('date').str.to_date()).lazy()
    if file_exists(apple_file):
        _existing_df = await read_df(apple_file)
        _all_apple_data = pl.concat([_existing_df, _df], how='align').unique()
    else:
        _all_apple_data = _df

    _all_apple_data.sort(by='dt').collect().write_parquet(apple_file)
    return


@app.cell(hide_code=True)
async def count_apple_health_size(apple_file, file_exists, mo, pl, read_df):
    _count = 0
    if file_exists(apple_file):
        _df = await read_df(apple_file)
        _count = _df.select(pl.len()).collect().item()
    mo.md(f'Antal Apple Hälsa datapunkter tillänglig {_count}')
    return


@app.cell(column=5, hide_code=True)
def funbeat_import_info(mo):
    mo.md(r"""
    ### Import av HTML (Funbeat) och uppdatera Garmin dataset med detta
    """)
    return


@app.cell(hide_code=True)
def import_funbeat_html_file(mo):
    funbeat_html_file = mo.ui.file_browser(label='Funbeat HTML fil', filetypes=['.htm', '.html'], limit=300).form()
    funbeat_html_file
    return (funbeat_html_file,)


@app.cell(hide_code=True)
def funbeat_load_as_df(funbeat_html_file, mo, pl):
    mo.stop(not funbeat_html_file.value)
    import pandas as pd
    funbeat_pd = pd.read_html(funbeat_html_file.value[0].path.as_posix()).pop()
    funbeat_df = pl.from_pandas(funbeat_pd).lazy()
    return (funbeat_df,)


@app.cell(hide_code=True)
def funbeat_adjust_to_garmin_df(funbeat_df, pl):
    # funbeat_df
    funbeat_df_modified = funbeat_df.with_columns(
        [
            pl.col('Träningsform').map_elements(lambda x: {'Löpning': 'running', 'Promenad': 'walking'}.get(x), return_dtype=pl.String).alias("activityType.typeKey"),
            (pl.col('Minuter') * 60 + pl.col('Sekunder')).alias('duration'),
            (pl.col('Sträcka (km)') * 10).alias('distance'),
            pl.col('Kcal').alias('calories'),
            pl.col('Runda').alias('activityName'),
            pl.col('Datum').str.to_datetime().alias('dt')
        ]).filter((pl.col("activityType.typeKey").is_not_null() & pl.col('dt').le(pl.datetime(year=2014, month=10, day=1)))).select('dt', 'distance', 'duration', 'calories', 'activityType.typeKey').sort(by='dt')
    return (funbeat_df_modified,)


@app.cell(hide_code=True)
def funbeat_df_merge_with_garmin(
    all_garmin_df,
    funbeat_df_modified,
    funbeat_html_file,
    mo,
    pl,
):
    mo.stop(not funbeat_html_file.value)
    garmin_with_funbeat = pl.concat([all_garmin_df, funbeat_df_modified], how='diagonal').sort(by='dt').unique()
    return (garmin_with_funbeat,)


@app.cell(hide_code=True)
def funbeat_filter_non_relevant_data(garmin_with_funbeat, pl):
    garmin_with_funbeat_fix_dist = garmin_with_funbeat.with_columns(
        pl.when((pl.col('dt').le(
            pl.date(year=2015, month=1, day=1)) & pl.col.distance.le(900)))
                .then(pl.col('distance') * 100)
                .otherwise(pl.col('distance')).alias('distance')).filter((pl.col.distance.gt(100) & pl.col.duration.gt(10) & (pl.col.distance / pl.col.duration).le(10)))
    # garmin_with_funbeat_fix_dist
    return (garmin_with_funbeat_fix_dist,)


@app.cell(hide_code=True)
def funbeat_save_to_garmin_file(garmin_file, garmin_with_funbeat_fix_dist):
    garmin_with_funbeat_fix_dist.collect().write_parquet(garmin_file)
    return


@app.cell(column=6, hide_code=True)
def jefit_intro(mo):
    mo.md(r"""
    ### Import av Jefit (gym) data via JSON

    Ex. indata

    ```json
    [
      {
        "date": "2017-10-03",
        "excercise": "Machine Reverse Flyes",
        "rep_max": "45",
        "sets": [
          "60x12",
          "60x12",
          "60x12"
        ]
      },
      {
        "date": "2017-10-15",
        "excercise": "Barbell Bench Press",
        "rep_max": "81.67",
        "sets": [
          "70x5",
          "60x8",
          "60x8"
        ]
      },
        ...
    ]
    ```
    """)
    return


@app.cell(hide_code=True)
def jefit_select_file_import(mo):
    jefit_import_file = mo.ui.file_browser(filetypes=['.json']).form()
    jefit_import_file
    return (jefit_import_file,)


@app.cell(hide_code=True)
def jefit_import_file(jefit_import_file, mo, pl):
    mo.stop(jefit_import_file.value is None, mo.md('Välj en Jefit JSON file att ladda upp'))
    jefit_raw_df = pl.read_json(jefit_import_file.value[0].path).lazy().with_columns(pl.col('date').str.to_date().alias('dt')).sort(by='dt')
    return (jefit_raw_df,)


@app.cell(hide_code=True)
def jefit_merge_with_existing(
    Path,
    jefit_file,
    jefit_import_file,
    jefit_raw_df,
    mo,
    pl,
):
    mo.stop(jefit_import_file.value is None, mo.md('Välj en Jefit JSON file att ladda upp'))
    if Path(jefit_file).exists():
        current_jefit = pl.scan_parquet(jefit_file)
        merged_jefit = pl.concat([current_jefit, jefit_raw_df], how='diagonal').unique()
        merged_jefit.collect().write_parquet(jefit_file)
    else:
        merged_jefit = jefit_raw_df
        merged_jefit.collect().write_parquet(jefit_file)
    return


@app.cell(hide_code=True)
def jefit_display_imported():
    return


@app.cell(column=7)
def display_info_jefit_csv(mo):
    mo.md(r"""
    ### Import av Jefit Export (gym) data via CSV
    """)
    return


@app.cell(hide_code=True)
def select_jefit_csv_import(mo):
    jefit_csv_input_file = mo.ui.file_browser(filetypes=['.csv'], limit=300).form()
    jefit_csv_input_file
    return (jefit_csv_input_file,)


@app.cell(hide_code=True)
def import_jefit_csv(jefit_csv_input_file, mo, pl):
    mo.stop(jefit_csv_input_file.value is None, mo.md('Välj en fil att importera'))
    lines_to_process = []

    f = open(jefit_csv_input_file.value[0].path)
    while line := next(f):
        if 'EXERCISE LOGS' in line:
            break

    next(f)  # Skip one line

    while line := next(f):
        if line.startswith('#') or len(line) <= 2:
            break
        lines_to_process.append(line)
    f.close()

    _jefit_raw_data = ''.join(lines_to_process)

    _jefit_df = pl.read_csv(_jefit_raw_data.encode()).lazy()

    df_imported_jefit = (
        _jefit_df.with_columns(
        pl.col('mydate').str.to_date().alias('dt'),
        pl.col('ename').alias('excercise'),
        pl.col('record').alias('rep_max').cast(dtype=pl.Float32),
        pl.col('logs').str.split(',').alias('sets')
        )
        .select(['dt', 'excercise', 'rep_max', 'sets'])
    )

    # df_imported_jefit
    return (df_imported_jefit,)


@app.cell(hide_code=True)
def save_jefit_csv_to_file(
    Path,
    df_imported_jefit,
    jefit_df,
    jefit_file,
    mo,
    pl,
):
    _merged_jefit_with_imported_df = pl.concat([jefit_df, df_imported_jefit]).unique(['dt', 'excercise', 'rep_max'], keep='first').sort(by='dt', descending=False)
    _merged_jefit_with_imported_df

    if Path(jefit_file).exists():
        _merged_jefit_with_imported_df.collect().write_parquet(jefit_file)
        mo.output.append(f'Sparade data till {jefit_file}')
    else:
        _merged_jefit_with_imported_df.collect().write_parquet(jefit_file)
    return


if __name__ == "__main__":
    app.run()
