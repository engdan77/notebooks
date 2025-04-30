# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "altair==5.5.0",
#     "appdirs==1.4.4",
#     "apple-health==2.0.0",
#     "dotenv==0.9.9",
#     "duckdb==1.2.2",
#     "garminconnect==0.2.26",
#     "loguru==0.7.3",
#     "marimo",
#     "metrics-collector==0.2.0",
#     "openai==1.73.0",
#     "pandas==2.2.3",
#     "persist-cache==0.4.3",
#     "polars[pyarrow]==1.27.1",
#     "python-dotenv==1.1.0",
#     "sqlalchemy==2.0.40",
#     "sqlglot==26.13.0",
#     "vegafusion==2.0.2",
#     "vl-convert-python==1.7.0",
#     "wat==0.6.0",
# ]
# ///

import marimo

__generated_with = "0.13.2"
app = marimo.App(width="full")


@app.cell
def imports():
    import marimo as mo
    import wat
    import polars as pl
    import altair as alt
    import json
    import dotenv
    import os
    import datetime
    from pathlib import Path
    from tempfile import NamedTemporaryFile
    from zipfile import ZipFile
    from health import HealthData
    from pathlib import Path
    return (
        HealthData,
        NamedTemporaryFile,
        Path,
        ZipFile,
        alt,
        datetime,
        dotenv,
        mo,
        os,
        pl,
    )


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


@app.cell
def _(dotenv, os):
    dotenv.load_dotenv('.env')
    username = os.getenv('GARMIN_USERNAME')
    password = os.getenv('GARMIN_PASSWORD')
    return password, username


@app.cell(hide_code=True)
def form(mo):
    form = mo.md('''
    Date range: {date_range}
    ''').batch(
        date_range=mo.ui.date_range()
    ).form()

    form
    return (form,)


@app.cell(hide_code=True)
def get_date_range_from_form(datetime, form, mo):
    mo.stop(form.value is None, mo.md('Fyll i data'))
    start_date, end_date = form.value['date_range']
    date_range = [start_date + datetime.timedelta(days=i) for i in range((end_date - start_date).days + 1)]
    date_range_iso = [date.isoformat() for date in date_range]
    return date_range_iso, end_date, start_date


@app.cell(hide_code=True)
def garmin_login(logger, password, username):
    from garminconnect import Garmin
    logger.debug('Logging in to Garmin')
    gc = Garmin(username, password)
    gc_login = gc.login()
    return (gc,)


@app.cell(hide_code=True)
def get_garmin_raw_data(datetime, gc, logger, mo):
    from persist_cache import cache

    RawGarminData = dict
    DateLike = str | datetime.date

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


    def get_raw_garmin_data(dt: DateLike | list[DateLike], pb=None) -> RawGarminData:
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
                bar.update()
        return all_activities


    def get_garmin_profile() -> dict:
        return gc.get_user_profile()


    # get_raw_garmin_data(username=username, password=password, dt=['2025-04-12', '2025-04-14'])
    dob_ = get_garmin_profile()['userData']['birthDate']
    dob = datetime.date.fromisoformat(dob_).year
    return RawGarminData, dob, get_raw_garmin_data


@app.cell(hide_code=True)
def get_garmin_df_and_filter(
    RawGarminData,
    date_range_iso,
    datetime,
    dob,
    form,
    get_raw_garmin_data,
    mo,
    pl,
):
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


    def convert_garmin_activities(raw_garmin_data: RawGarminData, dob_: int) -> pl.DataFrame:
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


    mo.stop(form.value is None, 'Fyll i data')
    _raw = get_raw_garmin_data(dt=date_range_iso)
    try:
        garmin_activities = convert_garmin_activities(_raw, dob_=dob).with_columns(
        pl.col('startTimeLocal').str.strptime(pl.Datetime, format="%Y-%m-%dT%H:%M:%S%.f").alias('dt')
    )
    except pl.exceptions.ColumnNotFoundError:
        m = 'Empty dataframe - skipping'
        mo.stop(True, mo.md(m))
    # Move dt column to 1st and sort by it
    cols = garmin_activities.columns
    garmin_activities = garmin_activities.select(["dt"] + [col for col in cols if col != "dt"]).sort(by="dt").filter(pl.col('distance') > 1000)

    return displayed_years, garmin_activities


@app.cell(hide_code=True)
def save_garmin_data(Path, garmin_activities, pl):
    current_garmin_data = garmin_activities
    _fn = 'all_health_garmin'
    if Path(_fn).exists():
        existing_df = pl.read_ndjson(_fn, schema_overrides={"dt": pl.Datetime})
        all_garmin_data = pl.concat([existing_df, current_garmin_data], how='align').unique().sort_by('dt')
    else:
        all_garmin_data = current_garmin_data
    all_garmin_data.write_ndjson(f'{_fn}.ndjson')
    all_garmin_data.write_parquet(f'{_fn}.parquet')
    return (current_garmin_data,)


@app.cell
def explore_garmin_dataset(current_garmin_data, mo):
    mo.ui.dataframe(current_garmin_data, page_size=10)
    return


@app.cell(hide_code=True)
def get_activities_as_chart(current_garmin_data, pl):
    activities_dist = current_garmin_data.rename({"activityType.typeKey": 'activity'}).group_by(pl.col('activity')).agg(pl.len().alias('count'))
    activities_dist.plot.bar(y='activity:N', x='count:Q').properties(height=100, title='Antal aktiviteter av typ')
    return (activities_dist,)


@app.cell(hide_code=True)
def select_activity_for_pulse_zones_chart(activities_dist, mo):
    interval_categories = {'dag': '1d', 'vecka': '1w', 'månad': '1mo', 'år': '1y'}

    activity_types_ = activities_dist.select('activity').to_series().to_list()
    activity_for_zones = mo.ui.dropdown(activity_types_, label='Aktivitet för se pulszoner')

    form_text = mo.md('Se aktiviteter av typ ... {activity_input} ... grupp per {interval_input} ...')

    graph_form = form_text.batch(activity_input=mo.ui.dropdown(activity_types_), interval_input=mo.ui.dropdown(interval_categories))

    graph_form
    # activity_for_zones
    return graph_form, interval_categories


@app.cell(hide_code=True)
def get_median_pulse_zones_chart(
    alt,
    current_garmin_data,
    end_date,
    graph_form,
    interval_categories,
    mo,
    pl,
    start_date,
):
    mo.stop(any(_ is None for _ in graph_form.value.values()) is True, mo.md('Välj aktivitet för zoner'))

    activity_input = graph_form['activity_input'].value
    interval_input = graph_form['interval_input'].value

    month_text = [k for k, v in interval_categories.items() if v == interval_input].pop()

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
    )

    _colors = {'median_zone1': 'gray', 'median_zone2': 'lightblue', 'median_zone3': 'green', 'median_zone4': 'orange', 'median_zone5': 'red'}


    interval_median_zones_chart = alt.Chart(interval_median_zones).transform_fold(
        ["median_zone1", "median_zone2", "median_zone3", "median_zone4", "median_zone5"],
        as_=['zone', 'median_time']
    ).mark_bar().encode(
        x=alt.X('yearmonth(dt_interval):T', title='Månad', scale=alt.Scale(domain=[start_date, end_date])),
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
    return (
        activity_input,
        interval_input,
        interval_median_zones_chart,
        month_text,
    )


@app.cell(hide_code=True)
def get_df_for_median_tempo(
    activity_input,
    current_garmin_data,
    graph_form,
    interval_input,
    mo,
    pl,
):
    mo.stop(any(_ is None for _ in graph_form.value.values()) is True, mo.md('Välj aktivitet för zoner'))
    # mo.stop(activity_for_zones.value is None, mo.md('Välj aktivitet för zoner'))

    df_activity_tempo = current_garmin_data.filter(pl.col('activityType.typeKey').eq(activity_input)).select('dt', 'distance', ((pl.col('duration')/60)/(pl.col('distance')/1000)).alias('mins_per_km'))

    chart_data_mins_per_km = df_activity_tempo.with_columns(pl.col('dt').dt.truncate(interval_input).alias('dt_interval')).group_by('dt_interval').agg(pl.col('mins_per_km').median().alias('mean_mins_per_km'))
    return chart_data_mins_per_km, df_activity_tempo


@app.cell(hide_code=True)
def get_chart_zones_and_temp(
    alt,
    chart_data_mins_per_km,
    end_date,
    graph_form,
    interval_median_zones_chart,
    mo,
    pl,
    start_date,
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
    mo.stop(any(_ is None for _ in graph_form.value.values()) is True, mo.md('Välj aktivitet för zoner'))

    min_tempo = chart_data_mins_per_km.select('mean_mins_per_km').min()['mean_mins_per_km'].first() - 0.5
    max_tempo = chart_data_mins_per_km.select('mean_mins_per_km').max()['mean_mins_per_km'].first() + 0.5

    median_km_per_hour_chart = alt.Chart(chart_data_mins_per_km.filter(pl.col('dt_interval') >= start_date)).mark_line(
        strokeWidth=5,
        color='red',
        ).encode(
        x=alt.X('dt_interval:T', scale=alt.Scale(domain=[start_date, end_date])),
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
    interval_input,
    month_text,
    pl,
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
            x=alt.X("dt_interval:T", title="Month"),
            y=alt.Y("activity_count:Q", title="Antal aktiviteter"),
            # color=alt.Color("distance_range:N", title="Distance Range"),
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
def chart_count_of_distances(activity_input, alt, current_garmin_data, pl):
    df_distance_in_km = current_garmin_data.filter(pl.col('activityType.typeKey').eq(activity_input)).select('dt', (pl.col('distance')/1000).alias('distance_km'))

    df_distance_in_km_rounded = df_distance_in_km.with_columns(pl.col('distance_km').round())
    df_grouped_ = df_distance_in_km_rounded.group_by(pl.col('distance_km')).agg(pl.len().alias('count')).sort(by='distance_km')

    alt.Chart(df_grouped_).mark_bar(size=20).encode(x=alt.X('distance_km:N', title='Kilometer'), y=alt.Y('count:Q', title='Antal')).properties(height=200, title='Antal aktiviteter grupperad på antal kilometer för perioden')

    return


@app.cell(hide_code=True)
def get_records_tempo(df_activity_tempo, mo, pl):
    def float_to_minutes_seconds(minutes_float):
        # Extract minutes
        minutes = int(minutes_float)
        # Extract seconds
        seconds = int((minutes_float - minutes) * 60)
        # Format as minutes:seconds
        return f"{minutes}:{seconds:02}"

    _df_with_times = df_activity_tempo.filter(pl.col('distance').is_between(5800, 6200)).with_columns(time=pl.col('mins_per_km').map_elements(float_to_minutes_seconds, return_dtype=pl.String).alias('tempo')).sort(by='mins_per_km', descending=False).select('dt', 'time').rename({'dt': 'Datum', 'time': 'Tempo (min/km)'})

    _table = mo.ui.table(_df_with_times, page_size=5, show_column_summaries=False)
    mo.output.append(mo.md('## Rekord hastighet för 6 km'))
    mo.output.append(_table)

    return


@app.cell(hide_code=True)
def get_records_distance(current_garmin_data, mo, pl):
    _longest_activities_df = current_garmin_data.select('dt', 'distance', 'activityType.typeKey', (pl.col('distance') / 1000).round().alias('km')).sort(by='distance', descending=True).rename({'activityType.typeKey': 'Aktivitet', 'dt': 'Datum'}).select('Datum', 'km', 'Aktivitet')

    mo.output.append(mo.md('## Rekord distanser för period'))
    mo.output.append(mo.ui.table(_longest_activities_df, show_column_summaries=False))
    return


@app.cell(hide_code=True)
def upload_apple_health(mo):
    mo.output.append('Ladda in Apple Hälsa data')
    apple_health_upload = mo.ui.file().form()
    mo.output.append(apple_health_upload)
    return (apple_health_upload,)


@app.cell(hide_code=True)
def process_apple_health_data(
    HealthData,
    NamedTemporaryFile,
    Path,
    ZipFile,
    apple_health_upload,
    logger,
    mo,
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

    return (apple_data,)


@app.cell(hide_code=True)
def display_apple_df(apple_data, mo, pl):
    apple_df = pl.DataFrame(apple_data).with_columns(dt=pl.col('date').str.to_date())
    mo.output.append(mo.md(f'## Apple Health data'))
    mo.output.append(apple_df)
    return (apple_df,)


@app.cell
def _(apple_df):
    apple_df.select('metric').unique()
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


@app.cell(hide_code=True)
def _(blood_pressure_data_grouped, interval_input, mo, month_text, pl):
    mo.output.append(mo.md(f'### Utforska blodtrycket för perioden med snitt per {month_text}'))

    blood_pressure_exploded = blood_pressure_data_grouped.pivot('metric', index="dt", values="value")

    blood_pressure_agg = blood_pressure_exploded.with_columns(pl.col('dt').dt.truncate(every=interval_input).alias('dt_interval')).group_by('dt_interval').agg(pl.mean(['bloodpressuresystolic', 'bloodpressurediastolic'])).sort(by='dt_interval')

    mo.output.append(blood_pressure_agg)
    return (blood_pressure_agg,)


@app.cell(hide_code=True)
def _(alt, blood_pressure_agg, month_text):
    _base = alt.Chart(blood_pressure_agg).mark_line().encode(
        x='dt_interval:T',
    ).properties(
        title=f'Medel blodtryck per {month_text}',
        width=600,
        height=300,
    )

    _dia = _base.encode(y=alt.Y('bloodpressurediastolic:Q', scale=alt.Scale(domainMin=40)), color=alt.value('lightblue'))
    _sys = _base.encode(y='bloodpressuresystolic:Q', color=alt.value('red'))

    _base + _dia + _sys
    return


@app.cell
def _(apple_df):
    apple_df
    return


@app.cell
def _(apple_df, pl):
    weight_fat_df = apple_df.filter(pl.col('metric').is_in(['bodymass', 'bodyfatpercentage'])).pivot('metric', index='dt', values='value')

    weight_fat_df
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
