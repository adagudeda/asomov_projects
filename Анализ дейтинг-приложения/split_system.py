import pandas as pd
import numpy as np

import matplotlib.pyplot as plt

from tqdm.auto import tqdm

def aa_ttest(data1, data2, stat, alpha=0.05, simulations=10000, n_s=500, replace=True):
    '''Функция для оценки качества системы спритования. Принимает два распределения исследуемой переменной
    data1, data2 - исследуемые переменные
    stat - статистический критерий
    alpha(0.05) - порог принятия решиний
    simulation(10000) - количество подвыборок
    n_s(500) - размер подвыборки
    replace(True) - выборки с возвращением или без'''
    
    res = []

    # Запуск симуляций A/A теста
    for i in tqdm(range(simulations)):
        s1 = data1.sample(n_s, replace=replace).values
        s2 = data2.sample(n_s, replace=replace).values
        res.append(stat(s1, s2)[1]) # сохраняем pvalue

    plt.hist(res, bins = 50)
    plt.xlabel('pvalues')
    plt.ylabel('frequency')
    plt.title("Histogram of ttest A/A simulations ")
    plt.show()

    # Проверяем, что количество ложноположительных случаев не превышает альфа
    return sum(np.array(res) < alpha) / simulations