import sys
import pandas as pd

# Getting args from cli
args = sys.argv
pipeline, name = args


version = pd.__version__

print(f'Hello, {name}')
print(f"We're running the {pipeline} file")
print(f'The actual pandas version is: {version}')