# Пакет Pyspark SQL

## Spark core

#### Глава 3  Программирование операций с RDD  

> 01_start.ipynb - стартовый проект  
> 02_textFile.ipynb - загрузка данных из текстового файла text.txt фильтр rdd по ключевому слову   
> 03_flatMap.ipynb - разбиение исходных строк на слова с помощью функции flatMap(), фильтрация filter и map (стр. 54, 56)  
> 04_operation_on_sets.ipynb - операции над множествами (стр. 57-59)  
> 05_actions.ipynb - примеры работы функций reduce, fold, aggregate, take, takeSample (стр. 58-63)  
> 06_persist.ipynb - пример использвования функции сохранения данных persist() (стр. 66-67)  
> 08_map_vs_flatMap_difference.ipynb - разница между реализациями функций RDD.map() и RDD.flatMap()  

#### Глава 4  Работа с парами ключ/значение  

> 07_key_value_pairs_01.ipynb - создание коллекции пар ключ-значение (стр. 69-73)  
> 07_key_value_pairs_02.ipynb - создание коллекции пар ключ-значение (стр. 74-77)  
>                       подсчет слов flatMap(), map(), reduceByKey()  
> 07_key_value_pairs_03.ipynb - настройка уровня параллелизма, группировка cogroup (стр. 78-80)  
> 07_key_value_pairs_04.ipynb - соединения (стр. 81-82)  
> 07_key_value_pairs_PageRank.ipynb - пример реализации алгоритма PageRank (стр. 91-93)
> 09_mapValues_vs_flatMapValues_difference.ipynb - разница между реализациями функций RDD.mapValues() и RDD.flatMapValues()

#### Глава 5  Загрузка и сохранение данных  

> 10_textFile_saveAsTextFile.ipynb - Загрузка и сохранение данных из обычных текстовых файлов (стр. 99-102)  
> 11_csv.ipjnb - загрузка и сохранение данных из csv файлов