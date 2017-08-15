# -*- coding: utf-8 -*-
# Natural Language Toolkit: Interface to the CRFSuite Tagger
#
# Copyright (C) 2001-2015 NLTK Project
# Author: Long Duong <longdt219@gmail.com>
# URL: <http://nltk.org/>
# For license information, see LICENSE.TXT

"""
A module for POS tagging using CRFSuite
"""
from __future__ import absolute_import
from __future__ import unicode_literals
import re 
from nltk.tag.api import TaggerI

try:
    import pycrfsuite
except ImportError:
    pass

class CRFTokenizer(TaggerI):
    def __init__(self,  feature_func = None, verbose = False, training_opt = {}):
        """
        Initialize the CRFSuite tagger 
        :param feature_func: The function that extracts features for each token of a sentence. This function should take 
        2 parameters: tokens and index which extract features at index position from tokens list. See the build in 
        _get_features function for more detail.   
        :param verbose: output the debugging messages during training.
        :type verbose: boolean  
        :param training_opt: python-crfsuite training options
        :type training_opt : dictionary 
        
        Set of possible training options (using LBFGS training algorithm).  
         'feature.minfreq' : The minimum frequency of features.
         'feature.possible_states' : Force to generate possible state features.
         'feature.possible_transitions' : Force to generate possible transition features.
         'c1' : Coefficient for L1 regularization.
         'c2' : Coefficient for L2 regularization.
         'max_iterations' : The maximum number of iterations for L-BFGS optimization.
         'num_memories' : The number of limited memories for approximating the inverse hessian matrix.
         'epsilon' : Epsilon for testing the convergence of the objective.
         'period' : The duration of iterations to test the stopping criterion.
         'delta' : The threshold for the stopping criterion; an L-BFGS iteration stops when the
                    improvement of the log likelihood over the last ${period} iterations is no greater than this threshold.
         'linesearch' : The line search algorithm used in L-BFGS updates:
                           { 'MoreThuente': More and Thuente's method,
                              'Backtracking': Backtracking method with regular Wolfe condition,
                              'StrongBacktracking': Backtracking method with strong Wolfe condition
                           } 
         'max_linesearch' :  The maximum number of trials for the line search algorithm.
         
        """
                   
        self._model_file = ''
        self._tagger = pycrfsuite.Tagger()
        
        if feature_func is None:
            self._feature_func =  self._get_features
        else:
            self._feature_func =  feature_func
        
        self._verbose = verbose 
        self._training_options = training_opt
        self._pattern = re.compile(r'\d')
        
    def set_model_file(self, model_file):
        self._model_file = model_file
        self._tagger.open(self._model_file)
            
    def _get_features(self, tokens, idx):
        return None;
        
    def tag_sent(self, sent):
        '''
        Tag a list of sentences. NB before using this function, user should specify the mode_file either by 
                       - Train a new model using ``train'' function 
                       - Use the pre-trained model which is set via ``set_model_file'' function  
        :params sentences : list of sentences needed to tag. 
        :type sentences : list(str)
        :return : list of tagged sentences. 
        :rtype : list (list (tuple(str,str))) 
        '''
        if self._model_file == '':
            raise Exception(' No model file is found !! Please use train or set_model_file function')
        
        # We need the list of sentences instead of the list generator for matching the input and output
        #features_=['S']*len(sent)
        features = [self._feature_func(sent,idx=i,labels=None) for i in range(len(sent))]
        #features=[item for item in features if len(item)>0]
        labels = self._tagger.tag(features)
                
        if len(labels) != len(sent):raise Exception(' Predicted Length Not Matched, Expect Errors !')
            
        tagged_sent = list(zip(sent,labels))
            
        return tagged_sent 
    
    def train(self, train_data, model_file):
        '''
        Train the CRF tagger using CRFSuite  
        :params train_data : is the list of annotated sentences.        
        :type train_data : list (list(tuple(str,str)))
        :params model_file : the model will be saved to this file.     
         
        '''
        trainer = pycrfsuite.Trainer(verbose=self._verbose)
        trainer.set_params(self._training_options)
        
        time = 0
        index = 0
        for sent in train_data:
            tokens,labels = zip(*sent)
            #print('{0}:{1}'.format(time,tokens))
            features = [self._feature_func(tokens,labels,i) for i in range(len(tokens))]
            trainer.append(features,labels)
            # print(len(features))
            # print(features)
            time += 1
            index += 1
            print(str(index) + "-----" + labels.__str__())
            print(tokens)

                        
        # Now train the model, the output should be model_file
        trainer.train(model_file)
        # Save the model file
        self.set_model_file(model_file) 

    def tag(self, tokens):
        '''
        Tag a sentence using Python CRFSuite Tagger. NB before using this function, user should specify the mode_file either by 
                       - Train a new model using ``train'' function 
                       - Use the pre-trained model which is set via ``set_model_file'' function  
        :params tokens : list of tokens needed to tag. 
        :type tokens : list(str)
        :return : list of tagged tokens. 
        :rtype : list (tuple(str,str)) 
        '''
        
        return self.tag_sent([tokens])[0]

def get_features_(tokens, labels, idx):
    '''
    特征抽取函数2
    U00:X[-2]
    U01:X[-1]
    U02:X[_]
    U03:X[+1]
    U04:X[+2]
    U05:X[-2] X[-1] X[_]
    U06:X[-1] X[_] X[+1]
    U07:X[_] X[+1] X[+2]
    U08:X[-1] X[_]
    U09:X[_] X[+1]
    '''
    len_tokens = len(tokens) if tokens is not None else 0
    feature_list = []
    if idx >= 2 and len_tokens > 2:
        feature_list.append('U00_' + tokens[idx - 2])
        feature_list.append('U05_' + '/'.join((tokens[idx - 2], tokens[idx - 1], tokens[idx])))
    if idx >= 1 and len_tokens > 1:
        feature_list.append('U01_' + tokens[idx - 1])
        feature_list.append('U08_' + '/'.join((tokens[idx - 1], tokens[idx])))
    feature_list.append('U02_' + tokens[idx])

    if idx >= 1 and len_tokens > 1 and idx + 1 <= len_tokens - 1:
        feature_list.append('U06_' + '/'.join((tokens[idx - 1], tokens[idx], tokens[idx + 1])))
    if idx + 1 <= len_tokens - 1:
        feature_list.append('U03_' + tokens[idx + 1])
        feature_list.append('U09_' + '/'.join((tokens[idx], tokens[idx + 1])))
    if idx + 2 <= len_tokens - 1:
        feature_list.append('U04_' + tokens[idx + 2])
        feature_list.append('U07_' + '/'.join((tokens[idx], tokens[idx + 1], tokens[idx + 2])))

    return feature_list

def from_tag_to_segment(tagged_str):
    segment_data = ''
    for tag in tagged_str:
        if tag[1].strip() == 'S':
            segment_data += tag[0] + ' '
        elif tag[1].strip() == 'B':
            segment_data += tag[0]
        elif tag[1].strip() == 'M':
            segment_data += tag[0]
        else:
            segment_data += tag[0] + ' '
    return segment_data

crf = CRFTokenizer(feature_func=get_features_)
model_ = 'segment_train_model.md'
crf.set_model_file(model_)

def get_segment(text):
    # text_data = text.decode('utf-8').strip('&nbsp&nbsp;;&nbs p\u3000\n')
    tag = crf.tag_sent(text)
    segment_text = from_tag_to_segment(tag)
    return segment_text

# if __name__ == "__main__":
    # import doctest
    # doctest.testmod(optionflags=doctest.NORMALIZE_WHITESPACE)
    #
    # print get_segment('我爱你，我的中国')