# data processing libraries
import numpy as np
import pandas as pd
import os

# webscraping annd HTML parsing libraries
import requests
import re
from bs4 import BeautifulSoup

# other libraries
import time
import nltk  
nltk.download("wordnet")

# very good tokenizer for english, considers sentence structure
from nltk.tokenize import TreebankWordTokenizer 
from nltk.stem import WordNetLemmatizer
from nltk.corpus import stopwords
from sklearn.feature_extraction.text import TfidfVectorizer  
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.feature_extraction.text import TfidfTransformer
from sklearn.naive_bayes import MultinomialNB
from imblearn.over_sampling import SMOTE
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.preprocessing import MaxAbsScaler
from sklearn.decomposition import PCA
from sklearn.linear_model import LogisticRegression
from imblearn.pipeline import Pipeline
from imblearn.over_sampling import RandomOverSampler
from imblearn.over_sampling import SMOTE
from imblearn.under_sampling import NearMiss
from sklearn.model_selection import GridSearchCV
from sklearn.decomposition import TruncatedSVD
from sklearn.ensemble import VotingClassifier
from sklearn.model_selection import train_test_split

# CONSTANTS
GORILLAZ_PATH = "/Users/elizavetabugaeva/Documents/Spiced/weekly_milestones/week_04/gorillaz"
NIRVANA_PATH = "/Users/elizavetabugaeva/Documents/Spiced/weekly_milestones/week_04/nirvana"

# FUNCTIONS
def get_all_lines(dir_path, all_lines):
    all_lines = []
    for filename in os.listdir(dir_path):
        if filename.endswith('.txt'):
            file_path = os.path.join(dir_path, filename)
            with open(file_path, 'r', encoding='utf-8') as f: 
                file_soup = BeautifulSoup(f, 'html.parser')
                lyrics_elem = file_soup.find('pre', class_="lyric-body")
                if lyrics_elem is not None:
                    for line in lyrics_elem.get_text().split('\n'):
                        line = line.strip("\r")  # remove carriage return
                        if len(line) > 0:
                            all_lines.append(line)
    return all_lines

# MAIN CODE
all_lines_gorillaz = []
gorillaz_func_lines = get_all_lines(GORILLAZ_PATH, all_lines_gorillaz)
all_lines_nirvana = []
nirvana_func_lines = get_all_lines(NIRVANA_PATH,all_lines_nirvana)

CORPUS = gorillaz_func_lines + nirvana_func_lines


vectorizer = TfidfVectorizer(stop_words='english')
vec = vectorizer.fit_transform(CORPUS)

feature_matrix = pd.DataFrame(
    vec.todense(), 
    columns=vectorizer.get_feature_names(), 
    index=LABELS
)

feature_matrix= feature_matrix.reset_index()
X = feature_matrix.drop('index', axis =1)
y = feature_matrix['index']
X_train_f, X_test_f, y_train_f, y_test_f = train_test_split(X, y, test_size=0.33, random_state=42)

#Log regression
lr_pipeline = Pipeline([
    ('PCA', PCA(n_components=0.99, svd_solver='full')),
    ('over', SMOTE(sampling_strategy={'nirvana':3500})),
    ('logregmodel', LogisticRegression(class_weight=None))
])
lr_pipeline.fit(X_train_f,y_train_f)

#Multionomial NB
NBmodel=MultinomialNB()
NBmodel.fit(X_train_f, y_train_f)

