# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "altair==5.5.0",
#     "marimo",
#     "openai==1.99.6",
#     "polars==1.32.2",
#     "pyarrow==21.0.0",
# ]
# ///

import marimo

__generated_with = "0.14.17"
app = marimo.App(
    width="columns",
    app_title="Energi kalkyl",
    layout_file="layouts/energy.grid.json",
)


@app.cell(column=0)
def _(mo):
    mo.md(r"""# Energi kalkylatorn""")
    return


@app.cell(hide_code=True)
def _():
    import marimo as mo
    import polars as pl
    import altair as alt
    import json
    import os
    import datetime
    import sys
    from pyarrow import parquet as pq
    import re
    from pathlib import Path
    import logging

    running_locally = isinstance(mo.notebook_location(), Path)
    energy_file = mo.notebook_location() / 'public' / 'energy_invoice_data.parquet'

    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger('energy')

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
        import base64
        mo.output.append(mo.md('Kör lokalt på dator'))
        # Workaround to avoid have Marimo tro to install packages in WASM mode
        install_packages = base64.b64decode(b'CmltcG9ydCBkb3RlbnYKZnJvbSB0ZW1wZmlsZSBpbXBvcnQgTmFtZWRUZW1wb3JhcnlGaWxlCmZyb20gemlwZmlsZSBpbXBvcnQgWmlwRmlsZQppbXBvcnQgZW5lcmd5bGVucwo=').decode()
        exec(install_packages)
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
            try:
                table = pq.read_table('data.parquet')
            except Exception:
                logger.error(f'Unable to read downloaded parquet file {loc}')
                return None
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
    return Path, alt, datetime, energy_file, is_wasm, mo, pl, read_df


@app.cell(hide_code=True)
def _(mo):
    params = mo.md('''
    Interval: {interval}
    ''').batch(interval=mo.ui.dropdown(['1m', '1y']))
    params
    return (params,)


@app.cell(hide_code=True)
def _(df3, mo, pl):
    df3_with_dates = df3.with_columns([
        pl.col("dt").dt.year().alias("year"),
        pl.col("dt").dt.month().alias("month")
    ])

    years_list = list(df3_with_dates['year'].unique().sort())

    select_years = mo.ui.multiselect(years_list)
    mo.output.append(mo.md('#### Ange vilka år att jämföra'))
    mo.output.append(select_years)
    return df3_with_dates, select_years


@app.cell
def _(df3):
    df3
    return


@app.cell
async def _(energy_file, read_df):
    df1 = await read_df(energy_file)
    return (df1,)


@app.cell
def _(df1, pl):
    def add_extra_columns_and_format_dt(_input_df: pl.DataFrame):
        service_col = "Stadsnät serviceavgift villa (kr/st)"
        _tmp_df = (_input_df.with_columns(
            (pl.col("Elnät totalt belopp (kr)") + 
             pl.col("Elhandel totalt belopp (kr)") + 
             pl.col("Fjärrvärme totalt belopp (kr)") +
             pl.when(pl.col(service_col) < 70).then(pl.col(service_col) * 1.25).otherwise(pl.col(service_col))).round().alias("Total kostnad"),
            pl.col('date').str.to_date().alias('dt')
        )).fill_nan(None).fill_null(strategy='backward').unique(subset='dt')
        return _tmp_df
    df3 = add_extra_columns_and_format_dt(df1)
    return add_extra_columns_and_format_dt, df3


@app.cell
def _(df3):
    df3.null_count()
    return


@app.cell
def _(df3):
    df3.columns
    return


@app.cell
def _(df3, mo, params, pl):
    mo.stop(params.value['interval'] is None, mo.md('Ange en intervall'))

    df4 = (df3.with_columns(pl.col('dt').dt.truncate(every=params.value['interval']).alias('period')).group_by('period')
        .agg(
            pl.col('Total kostnad').sum(),
            pl.col("El förbrukning (kWh)").sum().round(),
            pl.col("Fjärrvärme förbrukning (MWh)").sum().round(2),
            pl.col("Elnät överföring enkeltariff (öre/kWh)").mean().round(2),
            pl.col("Elnät energiskatt (öre/kWh)").mean().round(2),
            pl.col("Elhandel medelspotpris (öre/kWh)").mean().round(2),
            pl.col("Elhandel rörliga kostnader (öre/kWh)").mean().round(2),
            pl.col("Elhandel fasta påslag (öre/kWh)").mean().round(2),
            pl.col("Fjärrvärme energiavgift (kr/MWh)").mean().round(2)
        )
          )
    df4
    return (df4,)


@app.cell
def _(params):
    # To ensure bar charts stays within chart and increase bar widht
    range_min = 30
    range_max = 600
    _i = params.value['interval']
    bar_width = 5 if _i =='1m' else 40
    chart_width = 700
    return bar_width, chart_width, range_max, range_min


@app.cell
def _(alt, bar_width, df4, mo, params, range_max, range_min):
    mo.stop(params.value is None, mo.md('Välj en interval'))

    chart_cost_over_time = (alt.Chart(df4).mark_bar(size=bar_width).encode(
        x=alt.X('period:T', title='Period', scale=alt.Scale(rangeMin=range_min, rangeMax=range_max)),
        y=alt.Y('Total kostnad:Q', title='Kr'),
        tooltip=['period:T', 'Total kostnad:Q'])
        .properties(title='Total kostnad över tid', width=700)
        .configure_mark(color='steelblue')
    )


    chart_cost_over_time
    return


@app.cell
def _(alt, bar_width, chart_width, df4, range_max, range_min):
    # Create a base chart
    base = alt.Chart(df4).encode(x=alt.X('period:T', scale=alt.Scale(rangeMin=range_min, rangeMax=range_max)))

    # Create the line for "El förbrukning (kWh)"
    line_el = base.mark_bar(color='blue', size=bar_width).encode(y=alt.Y('El förbrukning (kWh):Q', title='🟦 El förbrukning (kWh)'), tooltip=['El förbrukning (kWh)', 'period'])

    # Create the line for "Fjärrvärme förbrukning (MWh)"
    line_fjarrvarme = base.mark_line(color='red', point=True).encode(y=alt.Y('Fjärrvärme förbrukning (MWh):Q', axis=alt.Axis(title='🟥 Fjärrvärme förbrukning (MWh)')), tooltip=['Fjärrvärme förbrukning (MWh)', 'period'])

    # Combine the two lines
    _chart = alt.layer(line_el, line_fjarrvarme).resolve_scale(
        y='independent'
    ).properties(
        title='El och Fjärrvärme förbrukning över tid',
        width=chart_width
    )

    _chart
    return


@app.cell
def _(alt, bar_width, chart_width, df4, range_max, range_min):
    # Melt the dataframe for stacked bar chart
    df4_melted = df4.unpivot(index='period', on=[
        'Elnät överföring enkeltariff (öre/kWh)', 
        'Elnät energiskatt (öre/kWh)', 
        'Elhandel medelspotpris (öre/kWh)', 
        'Elhandel rörliga kostnader (öre/kWh)', 
        'Elhandel fasta påslag (öre/kWh)'
    ], variable_name='Cost Type', value_name='Cost')

    # Create the stacked bar chart
    _chart = alt.Chart(df4_melted).mark_bar(size=bar_width).encode(
        x=alt.X('period:T', scale=alt.Scale(rangeMin=range_min, rangeMax=range_max)),
        y=alt.Y('Cost:Q', title='Kostnad (öre/kWh)', stack='zero'),
        color=alt.Color('Cost Type:N', title='Kostnad typ'),
        tooltip=['period:T', 'Cost Type:N', 'Cost:Q']
    ).properties(
        title='Rörliga pris kWh',
        width=chart_width,
    )

    _chart
    return


@app.cell
def _(alt, bar_width, chart_width, df4, range_max, range_min):
    _chart = alt.Chart(df4).mark_bar(size=bar_width).encode(
        x=alt.X('period:T', scale=alt.Scale(rangeMin=range_min, rangeMax=range_max)),
        y='Fjärrvärme energiavgift (kr/MWh):Q',
        tooltip=['period:T', 'Fjärrvärme energiavgift (kr/MWh):Q']
    ).properties(
        title='Fjärrvärme pris per MWh',
        width=chart_width,
    ).configure_mark(
        color='steelblue'
    )

    _chart
    return


@app.cell
def _(df3_with_dates, pl, select_years):
    df3_with_dates_selected = df3_with_dates.filter(pl.col('year').is_in(select_years.value))
    return (df3_with_dates_selected,)


@app.cell
def _(alt, df3_with_dates_selected, mo, select_years):
    mo.stop(not len(select_years.value), mo.md('Välj år'))

    # Creating a chart with year selection
    _year_selection = alt.selection_point(fields=['year'], bind='legend', name="Select Year", toggle=True)

    _line_chart = alt.Chart(df3_with_dates_selected).mark_line(point=True).encode(
        x=alt.X('month:O', title='Månad', axis=alt.Axis(labelExpr="['Jan', 'Feb', 'Mar', 'Apr', 'Maj', 'Jun', 'Jul', 'Aug', 'Sep', 'Okt', 'Nov', 'Dec'][datum.value - 1]")),
        y=alt.Y('Total kostnad:Q', title='Kr'),
        color=alt.Color('year:O', title='Year', scale=alt.Scale(scheme='category10')),
        tooltip=['dt:T', 'Total kostnad:Q']
    ).properties(
        title='Total kostnad över år',
        width=600,
        height=400
    ).add_params(
        _year_selection
    ).transform_filter(
        _year_selection
    )

    mo.output.append(_line_chart)
    return


@app.cell
def _(alt, df3_with_dates_selected, mo, select_years):
    mo.stop(not len(select_years.value), mo.md('Välj år'))

    # Creating a chart with year selection
    _year_selection = alt.selection_point(fields=['year'], bind='legend', name="Select Year", toggle=True)

    _line_chart = alt.Chart(df3_with_dates_selected).mark_line(point=True).encode(
        x=alt.X('month:O', title='Månad', axis=alt.Axis(labelExpr="['Jan', 'Feb', 'Mar', 'Apr', 'Maj', 'Jun', 'Jul', 'Aug', 'Sep', 'Okt', 'Nov', 'Dec'][datum.value - 1]")),
        y=alt.Y('El förbrukning (kWh):Q', title='kWh'),
        color=alt.Color('year:O', title='Year', scale=alt.Scale(scheme='category10')),
        tooltip=['dt:T', 'El förbrukning (kWh):Q']
    ).properties(
        title='Elförbrukning över år',
        width=600,
        height=400
    ).add_params(
        _year_selection
    ).transform_filter(
        _year_selection
    )

    mo.output.append(_line_chart)
    return


@app.cell
def _(alt, df3_with_dates_selected, mo, select_years):
    mo.stop(not len(select_years.value), mo.md('Välj år'))

    # Creating a chart with year selection
    _year_selection = alt.selection_point(fields=['year'], bind='legend', name="Select Year", toggle=True)

    _line_chart = alt.Chart(df3_with_dates_selected).mark_line(point=True).encode(
        x=alt.X('month:O', title='Månad', axis=alt.Axis(labelExpr="['Jan', 'Feb', 'Mar', 'Apr', 'Maj', 'Jun', 'Jul', 'Aug', 'Sep', 'Okt', 'Nov', 'Dec'][datum.value - 1]")),
        y=alt.Y('Fjärrvärme förbrukning (MWh):Q', title='MWh'),
        color=alt.Color('year:O', title='Year', scale=alt.Scale(scheme='category10')),
        tooltip=['dt:T', 'Fjärrvärme förbrukning (MWh):Q']
    ).properties(
        title='Fjärrvärm förbrukning över år',
        width=600,
        height=400
    ).add_params(
        _year_selection
    ).transform_filter(
        _year_selection
    )

    mo.output.append(_line_chart)
    return


@app.cell
def _(alt, datetime, df3_with_dates, mo, pl):
    last_year = datetime.date.today().year - 1

    _items = ["Fjärrvärme totalt belopp (kr)", "Elnät totalt belopp (kr)", "Elhandel totalt belopp (kr)"]

    _source_data = {df3_with_dates.filter(pl.col('year') == last_year)[i].sum() for i in _items}

    _source = pl.DataFrame({"Kategori": _items, "summa": list(_source_data)})

    # Calculate total and percentage
    total = _source['summa'].sum()
    _source = _source.with_columns((pl.col("summa") / total * 100).alias("percentage"))

    # Create a pie chart
    pie_chart = alt.Chart(_source).mark_arc().encode(
        theta="summa:Q",
        color="Kategori:N"
    ).properties(width=200, height=200)

    mo.output.append(pie_chart)

    _summary = []
    for k, v in zip(_items, _source_data):
        _summary.append(f'**{k}:** {round(v):_} kr/år')

    mo.output.append(mo.md(f'''
    ### Summering förra året {last_year}

    - {'\n\n- '.join(_summary)}
    '''))
    return


@app.cell
def _(datetime, df3_with_dates, mo, pl):
    this_month = datetime.date.today().month
    average_cost_month = round(df3_with_dates.filter(pl.col('month') == this_month)['Total kostnad'].mean())
    mo.md(f'''Denna månad beräknas kosta runt **{average_cost_month} kr**
    ''')

    return


@app.cell
def _():
    return


@app.cell(column=1, hide_code=True)
def _(mo):
    mo.md(
        r"""
    # Ladda in ny data

    Detta fungerar enbart om du kör Marimo lokalt på din dator, kommer be dig att logga in till din personliga Jönköping Energi portal med BankID.
    ```sh
    uvx --with https://github.com/engdan77/energylens.git marimo edit energy.py --headless --port 8081 --no-token
    ```
    """
    )
    return


@app.cell(hide_code=True)
def _(is_wasm, mo):
    if is_wasm():
        mo.stop(True, mo.md('Kör lokalt om du vill fortsätta'))
    else:
        mo.output.append(mo.md('''
        Kör lokalt
        '''))
        new_data_params = mo.md('''
        Antal månader bak att ladda ned: {month_count}
        ''').batch(month_count=mo.ui.number(start=1, value=3)).form()
        mo.output.append(new_data_params)
    return (new_data_params,)


@app.cell(hide_code=True)
async def _(
    Path,
    add_extra_columns_and_format_dt,
    df1,
    df3,
    energy_file,
    energylens,
    mo,
    new_data_params,
    new_df,
    pl,
):
    mo.stop(new_data_params.value is None, mo.md('Ange antal månader'))
    # import energylens
    import asyncio
    current_df = df1
    new_df_bytes = await energylens.async_get_last_invoices(count=new_data_params.value['month_count'])
    df_import = add_extra_columns_and_format_dt(pl.read_parquet(new_df))
    updated_df = pl.concat([df3, df_import], how='diagonal').unique()
    updated_df.write_parquet(energy_file.as_posix())
    mo.md(f'Uppdaterade {energy_file.as_posix()} [{Path(energy_file).stat().st_size:_} bytes]')
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
