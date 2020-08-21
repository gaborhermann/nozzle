# Nozzle

> "A nozzle is a device designed to control the direction or characteristics of a fluid flow (especially to increase velocity) as it exits (or enters) an enclosed chamber or pipe."
> - (Source: [Wikipedia](https://en.wikipedia.org/wiki/Nozzle))

![Image of Yaktocat](https://upload.wikimedia.org/wikipedia/commons/1/11/Water_nozzle.jpg)


Lightweight Python pipeline runner that can execute tasks respecting dependencies.
Like [Apache Airflow](https://github.com/apache/airflow), but more lightweight: no webservice, no scheduling, no templating.
A library, not a framework.
Plain Python, no dependencies.

## Background
Pet project to see how difficult it is to execute tasks respecting dependencies.
There are different tools for that. E.g. I've been using Apache Airflow, and it helps a lot,
but I was fed up with the complexity:
web interface, templating, overhead of starting a new process for every task, etc.
There are alternatives there (e.g. [Luigi](https://github.com/spotify/luigi))
but I was curious to see how difficult it is to write a tool that can execute
tasks respecting the dependencies, because in my opinion this is
the most important feature of these tools. Conclusion: it's fairly easy.

## Examples

Let's do some cooking!
```python
def chop_veggies_fn(): print("chop broccoli and carrot")
def steam_broccoli_fn(): print("steam broccoli")
def cook_carrot_fn(): print("cook carrot")
def wash_dishes_fn(): print("wash dishes")
```

We should first chop the veggies then cook them and wash the dishes after. 
It doesn't matter which order we steam the broccoli and cook the carrot,
we could even do these at the same time.

Nozzle to our help!
```python
from nozzle.dag import Dag, Operator
```

Let's define the steps as `Operator`s in a `Dag`,
```python
with Dag(id="dinner") as dag:
    chop_veggies = Operator(chop_veggies_fn)
    steam_broccoli = Operator(steam_broccoli_fn)
    cook_carrot = Operator(cook_carrot_fn)
    wash_dishes = Operator(wash_dishes_fn)
```

respect the order,
```python
chop_veggies >> [steam_broccoli, cook_carrot] >> wash_dishes
```

and we could start the cooking.
```python
from nozzle.runtime import run_dag
run_dag(dag, num_of_processes=4)
```
We're using a pool of 4 processes, so we can parallelize on cooking the broccoli and carrot.
We'll see:
```
chop veggies
cook carrot
steam broccoli
wash dishes
```
or
```
chop veggies
steam broccoli
cook carrot
wash dishes
```
because it doesn't matter if we steam the broccoli or cook the carrot first.

Full example:
```python
from nozzle.dag import Dag, Operator
from nozzle.runtime import run_dag


def chop_veggies_fn(): print("chop veggies")
def steam_broccoli_fn(): print("steam broccoli")
def cook_carrot_fn(): print("cook carrot")
def wash_dishes_fn(): print("wash dishes")


with Dag(id="dinner") as dag:
    chop_veggies = Operator(chop_veggies_fn)
    steam_broccoli = Operator(steam_broccoli_fn)
    cook_carrot = Operator(cook_carrot_fn)
    wash_dishes = Operator(wash_dishes_fn)
    chop_veggies >> [steam_broccoli, cook_carrot] >> wash_dishes

run_dag(dag, num_of_processes=4)
```

(Attentive reader might have noticed that definition is really similar to Airflow.)

### Handling errors
We can also handle errors: we'll stop the whole cooking process if we
don't have a steaming basket for the broccoli.
Let's change `steam_broccoli_fn` to raise an error:
```python
def steam_broccoli_fn(): raise RuntimeError("Oops, we don't have a steaming basket!")
```
then when we try running it:
```python
run_dag(dag, num_of_processes=4)
```
we'll see something like
```
Traceback (most recent call last):
  File "nozzle/runtime.py", line 66, in _worker
    op.python_callable(*op.args, **op.kwargs)
  File "<input>", line 2, in steam_broccoli_fn
RuntimeError: Oops, we don't have a steaming basket!
```

