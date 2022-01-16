##RATIONALE
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

## TODO

1. [ ] Integration Setup.
    * [ ] Check if csv files exists.
    * [ ] If not, then download them.
   
2. [ ] CLI
    * [ ] Integration Setup flag.
    * [ ] API Credentials flags.
    * [ ] Ingestion start flag.

3. [ ] Package
    * [ ] pyproject.toml and setup.cfg
    * [ ] entry points

5. [ ] API
    * [x] Interface
    * [ ] Define requests ops methods

6. [ ] CSV Manipulation
    * [x] Interface
    * [ ] Define pandas ops methods