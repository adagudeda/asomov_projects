import pandas as pd
import numpy as np

from scipy.stats import ttest_ind
from scipy.stats import mannwhitneyu

import matplotlib.pyplot as plt

from tqdm.auto import tqdm

def aa_ttest(data1, data2, alpha=0.05, simulations=10000, n_s=500, replace=True):
    '''Функция для оценки качества системы спритования. Принимает два распределения исследуемой переменной
    data1, data2 - исследуемые распределения
    alpha(0.05) - порог принятия решиний
    simulation(10000) - количество подвыборок
    n_s(500) - размер подвыборки
    replace(True) - выборки с возвращением или без'''
    
    res = []
    
    if n_s > min([len(data1), len(data2)]):
        n_s = min([len(data1), len(data2)])
    
    # Запуск симуляций A/A теста
    for i in tqdm(range(simulations)):
        s1 = data1.sample(n_s, replace=replace).values
        s2 = data2.sample(n_s, replace=replace).values
        res.append(ttest_ind(s1, s2, equal_var=False)[1]) # сохраняем pvalue

    plt.hist(res, bins = 20)
    plt.xlabel('pvalues')
    plt.ylabel('frequency')
    plt.title("Histogram of ttest A/A simulations ")
    plt.show()

    # Проверяем, что количество ложноположительных случаев не превышает альфа
    return sum(np.array(res) < alpha) / simulations