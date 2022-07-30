# Пакет Pyspark SQL

## Spark core

Архитектура Spark обеcпечивает линейное масштабирование: удвоение объемов доступных ресурсов часто приводит к удвоению скорости работы приложения.  

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

#### Глава 6  Дополнительные возможности Spark  

> 12_accumulators.ipynb - Использование переменных-аккумуляторов (стр. 130-135)  
> 13_broadcast_vars.ipynb - Использование широковещательных переменных (стр. 135-139)  
> 14_mapPartition.ipynb - Применение функции mapPartition() (стр. 141-142)  
> 15_number_operation_on_RDD.ipynb - Числовые операции над наборами RDD (стр. 146-)  
> 

#### Глава 8  Настройка и отладка Spark  

> 16_toDebugString.ipynb - Использование функции toDebugString() (вывод иерархий происхождения наборов RDD) (стр. 185)  
> Задание (Job) - это множество стадий, произведенных для конкретного действия

## Spark SQL

#### Использование Spark SQL в приложениях 

> 01_start_with_HiveContext.ipynb - стартовый проект, использующий HiveContext  
> 01_start_with_SQLContext.ipynb - стартовый проект, использующий SQLContext  
> 02_load_from_JSON.ipynb - загрузка данных из JSON файла  
> 03_load_from_PARQUET.ipynb - загрузка данных из PARQUET файла  