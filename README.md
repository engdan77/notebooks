# Notebooks



## 🙄 Daniels thoughts and reflections

This is placeholder for my own shareable *scientific notebooks*, great oportunity to excersice the muscle memory 💪🏻 using *Python* tools such as [Polars](https://pola.rs/) - great for working with large datasets and [Altair](https://altair-viz.github.io/) for making nice visual representation of the data. And big thanks to [Marimo](https://marimo.io/) to glue everything together into a notebook including widgets allow the user to select and change the output. What also makes it awesome is that Marimo does allow one to use [WebAssembly](https://en.wikipedia.org/wiki/WebAssembly) (WASM) so that allow user to run the notebook entirely within the browser with no other needs than a static web page. 😍



## 📕 Current Notebooks

### Year 2025

* **[Health Data](https://engdan77.github.io/notebooks/apps/daniels_health.html)**
  * This something I did develop before my annual doctors appointment allowing me to "share" my health data based on my custom view that I think I have best use of, and data collected across multiple sources such as [Garmin Connect](https://en.wikipedia.org/wiki/Garmin) - where my smartwatch uploads its data. And [Apple Health](https://en.wikipedia.org/wiki/Health_(Apple)) where my blood pressure meassures, weights are uploaded to. 🩺
* **[Energy consumption](https://engdan77.github.io/notebooks/apps/energy.html)**
  * As I found good reasons to do more in-depth analysis of our house energy consumption and costs involved so I made this notebook with charts to help more easily do this in the future. Click [here](https://engdan77.github.io/notebooks/apps/energy.html) to take part of this. ⚡️





*..... original instructions from the Marimo project .....* 

## 🚀 Turn Marimo Notebooks into Github Pages

1. Fork this repository
2. Add your marimo files to the `notebooks/` or `apps/` directory
   1. `notebooks/` notebooks are exported with `--mode edit`
   2. `apps/` notebooks are exported with `--mode run`
3. Push to main branch
4. Go to repository **Settings > Pages** and change the "Source" dropdown to "GitHub Actions"
5. GitHub Actions will automatically build and deploy to Pages

## Including data or assets

To include data or assets in your notebooks, add them to the `public/` directory.

For example, the `apps/charts.py` notebook loads an image asset from the `public/` directory.

```markdown
<img src="public/logo.png" width="200" />
```

And the `notebooks/penguins.py` notebook loads a CSV dataset from the `public/` directory.

```python
import polars as pl
df = pl.read_csv(mo.notebook_location() / "public" / "penguins.csv")
```

## 🧪 Testing

To test the export process, run `scripts/build.py` from the root directory.

```bash
python scripts/build.py
```

This will export all notebooks in a folder called `_site/` in the root directory. Then to serve the site, run:

```bash
python -m http.server -d _site
```

This will serve the site at `http://localhost:8000`.
