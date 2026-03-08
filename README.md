# olympus.py

This is a Python package for the Olympus project, which is a framework for building and deploying machine learning models. The package provides tools for data preprocessing, model training, and model evaluation.

## Installation
You can install the package using pip:

```bash
pip install olympus.py
```

## Usage
Here is a simple example of how to use the package:

```python
from olympus import DataPreprocessor, ModelTrainer, ModelEvaluator
# Create a data preprocessor
preprocessor = DataPreprocessor()
# Preprocess the data
X_train, y_train, X_test, y_test = preprocessor.preprocess_data('data.csv')
# Create a model trainer
trainer = ModelTrainer()
# Train the model
model = trainer.train(X_train, y_train)
# Create a model evaluator
evaluator = ModelEvaluator()
# Evaluate the model
accuracy = evaluator.evaluate(model, X_test, y_test)
print(f"Model accuracy: {accuracy}")
```