### Overall

<p>I divided the app into three main components:
Integration Setup, API and CSV manipulation.</p>
<p>Also, there is a model to validade the to-be
ingested product and a command line tool to navigate
through the app.</p>
<p>The core execution component is <em>ingestion.py</em>
which resembles a <em>facade</em> design pattern.</p>

### Assumptions 

1. One of the constraints is _unique __SKU___ but
is not explicit if it means, or what means _store_,
for all branches or only the specified ones. So I
decided to follow the API docs which the products with
the same SKU merges into only one SKU.
The app does that, and only the topmost expensive
of both specified branches .

## Usage

1. `git clone <repo> && cd <repo>`
2. `python -m venv <repo_path>/venv`
3. `source venv/bin/activate`

### Ingest

4. `pip install .`
5. `integration --setup`
6. `api-credentials --client-id mRkZGFjM --client-secret ZGVmMjMz`
7. `ingestion --start --merchant-ingest "Richard's" --merchant-update "Richard's" --merchant-delete Beauty`

### Tests

4. `pytest tests/`
5. `mypy src/`

## TODO

1. [x] Integration Setup.
    * [x] Check if csv files exists.
    * [x] If not, then download them.
   
2. [x] CLI
    * [x] Integration Setup flag.
    * [x] API Credentials flags.
    * [x] Ingestion start flag.

3. [x] Package
    * [x] pyproject.toml and setup.cfg
    * [x] entry points

4. [x] API
    * [x] Interface
    * [x] Define requests ops methods

5. [x] CSV Manipulation
    * [x] Interface
    * [x] Define pandas ops methods

6. [x] Model