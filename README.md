# Setup
https://github.com/iCentris/machine-learning/wiki/Environment-Setup


# Converting Jupyter Notebook to a Python script file

`jupyter` offers a cli command that can be used to convert the notebook to `*.py` file. `sh` into your docker process, then convert:

```
docker-compose run jupyter bash -c \
    'jupyter nbconvert --to script src/jupyter/topic-modeling-vibe.ipynb --output-dir src/'
```

# Learning Resources

* [Python Style Guide](https://www.python.org/dev/peps/pep-0008/? "PEP8")
* [Bandit](https://pypi.org/project/bandit/)
* [Flake8 Docs](https://buildmedia.readthedocs.org/media/pdf/flake8/latest/flake8.pdf)
* [Pytest](https://docs.pytest.org/en/latest/)