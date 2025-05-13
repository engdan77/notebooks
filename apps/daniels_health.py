# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "altair==5.5.0",
#     "apple-health==2.0.0",
#     "marimo",
#     "polars==1.29.0",
#     "pyarrow==20.0.0",
#     "python-dotenv==1.1.0",
#     "requests==2.32.3",
#     "wat==0.6.0",
# ]
# ///

import marimo

__generated_with = "0.13.6"
app = marimo.App(width="columns")


@app.cell(column=0, hide_code=True)
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
    import requests

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


    async def read_df(loc) -> pl.DataFrame:
        # Due to need workaround for remotely loading Parquete files - https://github.com/pola-rs/polars/issues/20876
        _df = None
        if not running_locally:
            mo.output.append(mo.md('Hämtar data från URL'))
            c = await async_get(loc)
            with open('data.parquet', 'wb') as f:
                f.write(c)
            table = pq.read_table('data.parquet')
            _df = pl.from_arrow(table)
        else: 
            _df = pl.read_parquet(loc)
        return _df


    def to_alt_dt(dt: datetime.datetime | datetime.date | str) -> alt.DateTime:
        if isinstance(dt, str):
            _dt = datetime.datetime.fromisoformat(dt)
        elif isinstance(dt, datetime.date):
            _dt = datetime.datetime(dt.year, dt.month, dt.day)
        elif isinstance(dt, datetime.datetime):
            _dt = dt
        return alt.DateTime(year=_dt.year, month=_dt.month, date=_dt.day, hours=_dt.hour, minutes=_dt.minute)
    return alt, datetime, file_exists, is_wasm, os, pl, read_df, to_alt_dt


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

    def padded(l: list, by: int):
        _x = [''] * by
        for i, item in enumerate(l):
            _x[i] = item
        return _x


    relevant_columns_dict = {'Garmin': relevant_garmin_colums, 'Apple': padded(relevant_apple_colums, by=len(relevant_garmin_colums))}
    mo.output.append(mo.md('### Kolumner som används för data'))
    mo.output.append(mo.ui.table(relevant_columns_dict, page_size=30))
    return relevant_apple_colums, relevant_garmin_colums


@app.cell(hide_code=True)
def define_global_vars(mo):
    garmin_file = mo.notebook_location() / 'public' / 'all_health_garmin.parquet'
    mo.output.append(mo.md(f'Garmin fil att använda `{garmin_file}`'))
    apple_file = mo.notebook_location() / 'public' / 'apple_health.parquet'
    mo.output.append(mo.md(f'Apple Hälsa fil att använda `{apple_file}`'))
    return apple_file, garmin_file


@app.cell(hide_code=True)
def form_for_display(mo):
    interval_categories = {'dag': '1d', 'vecka': '1w', 'månad': '1mo', 'år': '1y'}

    form = mo.md('''
    ## Ange detaljer för statistik

    Mellan datum: {date_range}

    Gruppera per {interval_input}  ... (upplösning)

    ''').batch(
        date_range=mo.ui.date_range(),
        interval_input=mo.ui.dropdown(interval_categories)
    )

    form
    return form, interval_categories


@app.cell(hide_code=True)
def select_activity_for_pulse_zones_chart(activities_dist, mo):
    activity_types = activities_dist.select('activity').to_series().to_list()
    _form_text = mo.md('Se aktiviteter av typ ... {activity_input} ...')
    activity_type_form = _form_text.batch(activity_input=mo.ui.dropdown(activity_types))
    activity_type_form
    return (activity_type_form,)


@app.cell(hide_code=True)
def get_data_periods_gantt(
    all_apple_df,
    all_garmin_df,
    end_date,
    mo,
    start_date,
):
    _chosen_years = end_date.year - start_date.year
    if not _chosen_years:
        _chosen_years = 1
    _apple_start = all_apple_df.select('dt')['dt'].min().year
    _apple_years = all_apple_df.select('dt')['dt'].max().year - _apple_start
    _garmin_start = all_garmin_df.select('dt')['dt'].min().year
    _garmin_years = all_garmin_df.select('dt')['dt'].max().year - _garmin_start

    _gantt = f'''gantt
        title Data över år
        Vald period: crit, {start_date.year}, {_chosen_years}y
        Apple: {_apple_start}, {_apple_years}y
        Garmin: {_garmin_start}, {_garmin_years}y
        '''
    mo.mermaid(_gantt)
    return


@app.cell(hide_code=True)
def set_month_text(interval_categories, interval_input, mo):
    mo.stop(interval_input is None)
    month_text = [k for k, v in interval_categories.items() if v == interval_input].pop()
    return (month_text,)


@app.cell(hide_code=True)
async def load_or_empty_current_garmin_data(
    end_date,
    file_exists,
    garmin_file,
    mo,
    pl,
    read_df,
    relevant_garmin_colums,
    start_date,
):
    if file_exists(garmin_file):
        _df = await read_df(garmin_file)
        all_garmin_df = _df
        current_garmin_data = _df.filter(pl.col('dt').is_between(start_date, end_date))
        _sorted_df = _df.select('dt').sort(by='dt')['dt']
        mo.output.append(f'Tillänglig Garmin data punkter finns för {_sorted_df.min():%Y-%m-%d} <-> {_sorted_df.max():%Y-%m-%d}')
    else:
        current_garmin_data = pl.DataFrame({k: [] for k in relevant_garmin_colums})
        all_garmin_df = current_garmin_data

    mo.output.append(mo.md(f'Antal Garmin datapunkter för vald period {current_garmin_data.height} mellan {start_date} <-> {end_date}'))

    return all_garmin_df, current_garmin_data


@app.cell(hide_code=True)
def get_activities_as_chart(current_garmin_data, pl):
    activities_dist = current_garmin_data.rename({"activityType.typeKey": 'activity'}).group_by(pl.col('activity')).agg(pl.len().alias('count'))
    activities_dist.plot.bar(y='activity:N', x='count:Q').properties(height=100, title='Antal aktiviteter av typ')
    return (activities_dist,)


@app.cell(hide_code=True)
def get_chart_zones_and_temp(
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
    '''
    This example will enforce pan/zoom that is not desired - so lean towards using Altair object
    median_km_per_hour_chart = chart_data_mins_per_km.plot.line(
        strokeWidth=alt.value(5),
        color=alt.value("red"),
        x=alt.X('month:T', scale=alt.Scale(domain=[
        first_dt_in_zone_chart, 
        last_dt_in_zone_chart,
    ])), y=alt.Y('mean_mins_per_km', scale=alt.Scale(domain=[3, 13]))).properties(
        title='Median min/km hastighet för aktivitet',
        width=600,
        height=400,
        strokeWidth=alt.value(10)
    )
    '''

    # mo.stop(activity_for_zones.value is None, mo.md('Välj aktivitet för zoner'))
    mo.stop(any(_ is None for _ in activity_type_form.value.values()) is True, mo.md('Välj aktivitet för zoner'))

    min_tempo = chart_data_mins_per_km.select('mean_mins_per_km').min()['mean_mins_per_km'].first() - 0.5
    max_tempo = chart_data_mins_per_km.select('mean_mins_per_km').max()['mean_mins_per_km'].first() + 0.5

    median_km_per_hour_chart = alt.Chart(chart_data_mins_per_km.filter(pl.col('dt_interval') >= start_date)).mark_line(
        strokeWidth=5,
        color='red',
        ).encode(
        x=alt.X('dt_interval:T', scale=alt.Scale(domain=[to_alt_dt(start_date), to_alt_dt(end_date)])),
        y=alt.Y('mean_mins_per_km', scale=alt.Scale(domain=[min_tempo, max_tempo]))   
    ).properties(
        title='Median min/km hastighet för aktivitet (tempo)',
        width=600,
        height=200,
        strokeWidth=10  
    )
    interval_median_zones_chart & median_km_per_hour_chart
    return


@app.cell(hide_code=True)
def get_count_distances_chart(
    activity_input,
    alt,
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
        .agg(pl.count("activityId").alias("activity_count"))
    )

    # Create a stacked bar chart
    chart_activity_distances = (
        alt.Chart(_activity_counts)
        .mark_bar()
        .encode(
            x=alt.X("dt_interval:T", title="Month", scale=alt.Scale(domain=[to_alt_dt(start_date), to_alt_dt(end_date)])),
            y=alt.Y("activity_count:Q", title="Antal aktiviteter"),
            color=alt.Color('distance_range:N', scale=alt.Scale(domain=list(_colors.keys()), range=list(_colors.values()))),
            tooltip=["dt_interval:O", "distance_range:N", "activity_count:Q"]
        )
        .properties(
            title=f"Antal aktiviteter med distanser per {month_text}",
            width=600,
            height=200
        )
    )

    chart_activity_distances.interactive()

    return


@app.cell(hide_code=True)
def get_walk_run_distance_chart(
    alt,
    end_date,
    month_text,
    start_date,
    to_alt_dt,
    walk_run_df,
):
    distance_chart = alt.Chart(walk_run_df).mark_bar().encode(x=alt.X('dt_interval', title='Datum', scale=alt.Scale(domain=[to_alt_dt(start_date), to_alt_dt(end_date)])), y=alt.Y('value', title='kilometer'), color=alt.Color('type', title='Kategori')).properties(
        title=f'Antal km per {month_text}',
        width=600,
        height=300
    )
    distance_chart
    return


@app.cell(hide_code=True)
def chart_count_of_distances(activity_input, alt, current_garmin_data, pl):
    df_distance_in_km = current_garmin_data.filter(pl.col('activityType.typeKey').eq(activity_input)).select('dt', (pl.col('distance')/1000).alias('distance_km'))

    df_distance_in_km_rounded = df_distance_in_km.with_columns(pl.col('distance_km').round())
    df_grouped_ = df_distance_in_km_rounded.group_by(pl.col('distance_km')).agg(pl.len().alias('count')).sort(by='distance_km')

    alt.Chart(df_grouped_).mark_bar(size=20).encode(x=alt.X('distance_km:N', title='Kilometer'), y=alt.Y('count:Q', title='Antal')).properties(height=200, title='Antal aktiviteter grupperad på antal kilometer för perioden')

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

    _df_with_times = df_activity_tempo.filter(pl.col('distance').is_between(5800, 6200)).with_columns(time=pl.col('mins_per_km').map_elements(float_to_minutes_seconds, return_dtype=pl.String).alias('tempo')).sort(by='mins_per_km', descending=False).select('dt', 'time').rename({'dt': 'Datum', 'time': 'Tempo (min/km)'})

    if not is_wasm():
        _table = mo.ui.table(_df_with_times, page_size=5, show_column_summaries=False)
    else:
        _table = mo.plain(_df_with_times)

    mo.output.append(mo.md('## Rekord hastighet för 6 km'))
    mo.output.append(_table)

    return


@app.cell(hide_code=True)
def get_records_distance(current_garmin_data, is_wasm, mo, pl):
    _longest_activities_df = current_garmin_data.select('dt', 'distance', 'activityType.typeKey', (pl.col('distance') / 1000).round().alias('km')).sort(by='distance', descending=True).rename({'activityType.typeKey': 'Aktivitet', 'dt': 'Datum'}).select('Datum', 'km', 'Aktivitet')

    mo.output.append(mo.md('## Rekord distanser för period'))

    if not is_wasm():
        _t = mo.ui.table(_longest_activities_df, show_column_summaries=False)
    else:
        _t = mo.plain(_longest_activities_df)

    mo.output.append(_t)
    return


@app.cell(hide_code=True)
def explore_garmin_dataset(current_garmin_data, is_wasm, mo):
    if is_wasm():
        mo.plain(current_garmin_data)
    else:
        mo.ui.dataframe(current_garmin_data, page_size=10)
    return


@app.cell(hide_code=True)
def get_walking_distance_from_apple_df(apple_df, interval_input, mo, pl):
    mo.stop(apple_df.height == 0 or not interval_input)

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
    mo.stop(current_garmin_data.height == 0 or not interval_input)

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
    activity_type_form,
    alt,
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
    # interval_input = graph_form['interval_input'].value

    # month_text = [k for k, v in interval_categories.items() if v == interval_input].pop()

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

    interval_median_zones_chart = alt.Chart(interval_median_zones).transform_fold(
        ["median_zone1", "median_zone2", "median_zone3", "median_zone4", "median_zone5"],
        as_=['zone', 'median_time']
    ).mark_bar().encode(
        x=alt.X('yearmonth(dt_interval):T', title='Månad', scale=alt.Scale(domain=[to_alt_dt(start_date), to_alt_dt(end_date)])),
        y=alt.Y('median_time:Q', title='Median tid i minuter'),
        color=alt.Color('zone:N', scale=alt.Scale(
            domain=list(_colors.keys()), range=list(_colors.values())), 
            title='Heart Rate Zones'),
        tooltip=['dt_interval:T', 'zone:N', 'median_time:Q'],
        order=alt.Order('zone:N', sort='ascending')
    ).properties(
        title=f'Median tid per aktivitet & tider i puls zoner för {month_text}',
        width=600,
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


@app.cell
def _(current_garmin_data, pl):
    fastest_6km_runs = current_garmin_data.filter((pl.col('activityType.typeKey') == 'running') & (pl.col('distance').is_between(5800, 6200))).select((pl.col('duration') / 60).round().alias('Minuter'), pl.col('dt').dt.date().alias('datum')).sort(by='Minuter', descending=False).limit(10)

    longest_running_distances = current_garmin_data.filter(pl.col('activityType.typeKey') == 'running').sort(by='distance', descending=True).select((pl.col('distance') / 1000).round(1).alias('km'), pl.col('dt').dt.date().alias('datum')).limit(10)
    return fastest_6km_runs, longest_running_distances


@app.cell
def _(fastest_6km_runs, longest_running_distances, mo):
    mo.output.append(mo.md('### Längsta löpningen 🥇'))
    mo.output.append(mo.plain(longest_running_distances))
    mo.output.append(mo.md('### Snabbast 6km löpningen 🥇'))
    mo.output.append(mo.plain(fastest_6km_runs))
    return


@app.cell
def _():
    return


@app.cell(column=1, hide_code=True)
def display_blood_pressure_chart(
    alt,
    blood_pressure_agg,
    end_date,
    month_text,
    start_date,
    to_alt_dt,
):
    _base = alt.Chart(blood_pressure_agg).mark_line().encode(
        x=alt.X('dt_interval:T', title='Datum', scale=alt.Scale(domain=[to_alt_dt(start_date), to_alt_dt(end_date)])),
    ).properties(
        title=f'Medel blodtryck per {month_text}',
        width=600,
        height=300,
    )

    _dia = _base.encode(y=alt.Y('bloodpressurediastolic:Q', title='Dia', scale=alt.Scale(domainMin=40)), color=alt.value('lightblue'))
    _sys = _base.encode(y=alt.Y('bloodpressuresystolic:Q', title='Sys'), color=alt.value('red'))

    _base + _dia + _sys
    return


@app.cell(hide_code=True)
def display_weight_fat_plot(
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

    _base = alt.Chart(_df_weight).properties(width=600, height=300)

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
def _(blood_pressure_agg, mo, pl):
    mo.output.append(mo.md('### Högsta mätningar av blodtryck 🩸'))
    mo.output.append(mo.plain(blood_pressure_agg.select(pl.col('dt_interval').alias('datum'), pl.col('bloodpressuresystolic').round(1), pl.col('bloodpressurediastolic').round(1)).sort(by='bloodpressurediastolic', descending=True).limit(5)))

    mo.output.append(mo.md('### Lägsta mätningar av blodtryck 🩸'))
    mo.output.append(mo.plain(blood_pressure_agg.select(pl.col('dt_interval').alias('datum'), pl.col('bloodpressuresystolic').round(1), pl.col('bloodpressurediastolic').round(1)).sort(by='bloodpressurediastolic', descending=False).limit(5)))
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
    mo.stop(interval_input is None)

    mo.output.append(mo.md(f'### Utforska blodtrycket för perioden med snitt per {month_text}'))

    blood_pressure_exploded = blood_pressure_data_grouped.pivot('metric', index="dt", values="value")

    blood_pressure_agg = blood_pressure_exploded.with_columns(pl.col('dt').dt.truncate(every=interval_input).alias('dt_interval')).group_by('dt_interval').agg(pl.mean(['bloodpressuresystolic', 'bloodpressurediastolic'])).sort(by='dt_interval')

    mo.output.append(blood_pressure_agg)
    return (blood_pressure_agg,)


@app.cell(hide_code=True)
async def load_apple_df(
    apple_file,
    end_date,
    file_exists,
    is_wasm,
    mo,
    pl,
    read_df,
    relevant_apple_colums,
    start_date,
):
    if file_exists(apple_file):
        if is_wasm():
            all_apple_df = await read_df(apple_file)
        else:
            all_apple_df = pl.read_parquet(apple_file)

        _df = all_apple_df.filter(pl.col('dt').is_between(start_date, end_date))
        _dt = all_apple_df.select('dt').sort(by='dt')['dt']
        mo.output.append(mo.md(f'All Apple Hälsa data finns för perioden {_dt.min():%Y-%m-%d} <-> {_dt.max():%Y-%m-%d}'))
    else:
        _df = pl.DataFrame({k: [] for k in relevant_apple_colums})
        all_apple_df = _df

    apple_df = _df
    mo.output.append(mo.md(f'Antal Apple datapunkter för vald period {apple_df.height} mellan {start_date} <-> {end_date}'))
    return all_apple_df, apple_df


@app.cell(hide_code=True)
def display_apple_df(apple_df, is_wasm, mo):
    # apple_df = pl.DataFrame(apple_data).with_columns(dt=pl.col('date').str.to_date())
    mo.output.append(mo.md(f'## Utforska all Apple Hälsa data'))
    if not is_wasm():
        mo.output.append(mo.ui.dataframe(apple_df))
    else:
        mo.output.append(mo.plain(apple_df))
    return


@app.cell(hide_code=True)
def get_weight_fat_df_from_apple_df(apple_df, interval_input, mo, pl):
    mo.stop(apple_df.height == 0 or not interval_input)

    weight_fat_df = apple_df.filter(pl.col('metric').is_in(['bodymass', 'bodyfatpercentage'])).pivot('metric', index='dt', values='value', aggregate_function='mean').with_columns(pl.col('dt').dt.truncate(every=interval_input).alias('dt_interval')).group_by('dt_interval').agg(pl.mean(['bodymass', 'bodyfatpercentage']))
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
def start_garmin_download(mo):
    mo.output.append(mo.md('## Ladda in Garmin aktiviteter från API'))
    input_run_garmin_import = mo.ui.run_button(label='Starta')
    mo.output.append(input_run_garmin_import)
    return (input_run_garmin_import,)


@app.cell(hide_code=True)
def get_garmin_credentials(dotenv, is_wasm, mo, os):
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
    garmin_password,
    garmin_username,
    is_wasm,
    logger,
    mo,
):
    mo.stop(not garmin_login_found, mo.md('Avaktar med att hämta Garmin data'))

    if not is_wasm:
        # Workaround ensure not attempt to micropip this while used as WASM
        exec('''from persist_cache import cache
    from garminconnect import Garmin
        ''')

    DateLike = str | datetime.date

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
    return dob, gc, get_raw_garmin_data


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
        # existing_df = pl.read_ndjson(_fn, schema_overrides={"dt": pl.Datetime})
        logger.info(f'Loading existing Garmin {garmin_file} and updating with existing')
        existing_df = await read_df(garmin_file)
        all_garmin_data = pl.concat([existing_df, garmin_activities], how='align').unique()
    else:
        logger.info('Building empty Garmin {garmin_file}')
        all_garmin_data = garmin_activities
    all_garmin_data.write_parquet(garmin_file)
    return


@app.cell(hide_code=True)
async def _(file_exists, garmin_file, mo, read_df):
    mo.stop(file_exists(garmin_file) is False)

    _df = await read_df(garmin_file)
    mo.output.append(_df)

    # _sorted = _df.select('dt').sort(by='dt')
    # _as_list = _sorted.select(pl.col('dt'))['dt']
    # _first, _last = _as_list.first(), _as_list.last()

    # mo.output.append(mo.md(f'''Garmin data just nu lagrad för perioden {_first:%Y-%m-%d} <-> {_last:%Y-%m-%d}'''))


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


@app.cell(column=3, hide_code=True)
def upload_apple_health(mo):
    mo.output.append('Ladda in Apple Hälsa data')
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

    _df = pl.DataFrame(apple_data).with_columns(dt=pl.col('date').str.to_date())
    if file_exists(apple_file):
        _existing_df = await read_df(apple_file)
        _all_apple_data = pl.concat([_existing_df, _df], how='align').unique()
    else:
        _all_apple_data = _df

    _all_apple_data.write_parquet(apple_file)

    return


@app.cell(hide_code=True)
async def count_apple_health_size(apple_file, file_exists, mo, read_df):
    _count = 0
    if file_exists(apple_file):
        _df = await read_df(apple_file)
        _count = _df.height
    mo.md(f'Antal Apple Hälsa datapunkter tillänglig {_count}')
    return


if __name__ == "__main__":
    app.run()
