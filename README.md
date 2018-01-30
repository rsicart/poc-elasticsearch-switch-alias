
# poc-elasticsearch-switch-alias

Poc to test automatisation of Elasticsearch alias switching

Requirements:
* python3
* Local Elasticsearch endpoint

Optional:
* Virtualenv

```
$ virtualenv -p /usr/bin/python3 venv
$ source venv/bin/activate
```

## Usage

**Runs with Python 3**

```
$ pip install -r requirements.txt

$ python3 switch_alias.py --help
usage: switch_alias.py [-h] --alias ALIAS --index INDEX

Run

optional arguments:
  -h, --help            show this help message and exit
  --alias ALIAS, -l ALIAS
                        Alias to switch
  --index INDEX, -i INDEX
                        New alias to point to
```
