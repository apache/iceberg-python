in the snapshot.py class, you define the "api" or logic to collect the changes and then stage, them.  Then the
__init__.py has a decorator that calls the type to actually apply the metadata changes.