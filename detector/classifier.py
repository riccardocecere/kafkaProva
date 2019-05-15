from sklearn.pipeline import Pipeline
from sklearn.preprocessing import LabelEncoder
from sklearn.feature_extraction.text import CountVectorizer, TfidfTransformer
from sklearn.naive_bayes import MultinomialNB
from sklearn import model_selection
import pickle
from pandas import read_csv, read_parquet
import text_parser


def prepare_training_set(dataset):
    column=[]
    for p in dataset['domain']:
        column.append(text_parser.get_html_text(str(p)))
    dataset['text']=column

'''load the machine learning model from the path below, if it doesn't exists it create the model and train it'''
def load_classifier(model_path, parquet_path, training_set_path):
    #Check if the model already exists
    import os
    exists = os.path.isfile(model_path)
    if not exists:
        print('Addestramento del modello....')
        #Check if the train parquet already exists
        exists_parquet = os.path.isfile(parquet_path)
        if not exists_parquet:
            print('Creazioe del parquet')
            #Load train CSV
            filename = '/data/training_set.csv'
            dataframe = read_csv(filename, sep=';', names=['domain','category'])
            #Convert CSV to dataframe
            prepare_training_set(dataframe)
            #Convert Dataframe to parquet and save to path 'parquet_name'
            dataframe.to_parquet(parquet_path, engine='auto', compression='snappy')
        #load train parquet
        dataset = read_parquet(parquet_path)

        #Encoding of the 'category' column to a new column 'label'
        le = LabelEncoder()
        dataset['label']=le.fit_transform(dataset['category'])
        #Pipeline composed by tokenizer, tfidf analizer and classifier
        nb = Pipeline([('vect', CountVectorizer()),
                       ('tfidf', TfidfTransformer()),
                       ('clf', MultinomialNB()),
                       ])
        #split of the training set
        X_train, X_test, Y_train, Y_test = model_selection.train_test_split(dataset['text'], dataset['label'], test_size = 0.2, random_state=69)

        #training of the model
        nb.fit(X_train, Y_train)
        print(nb.score(X_test, Y_test))
        #decoding of the column 'label' in to the column 'pred'
        dataset['pred']=le.inverse_transform(dataset['label'])
        print('salvo il modello')
        #Save the model to path 'filename'
        pickle.dump(nb, open(model_path, 'wb'))
    #Load the model
    filename = '/model/classifier.sav'
    loaded_model = pickle.load(open(model_path, 'rb'))

    return loaded_model

'''method that return the prediction value of the input given'''
def predict(model,input):
    return model.predict([input])

