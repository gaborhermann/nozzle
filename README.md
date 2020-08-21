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

## TODOs
- [x] Add Apache License
- [x] Rename Op to Operator
- [x] Add Python docs to methods
- [x] Add setup.py
- [x] Add Makefile for testing, packaging
- [x] Add >> operator override
- [x] Add PythonOperator
- [x] Write dag tests: regular DAG, circular DAG
- [x] Write runtime tests: empty DAG, order with a queue, failure handling
- [x] Add operator ids
- [ ] Add context managed Dag
- [ ] Write README
- [ ] Write blogpost with examples (linear DAG, empty DAG)
- [ ] Add DAG based on datestamp

