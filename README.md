Весь код записан в code.py
Основная работа над поставленной задачей производилась с помощью библиотеки geopy.
С её помощью я расчитал геодезическое расстояние между двумя задаными координатами (от центра ЖК до автобусных остановок).
Я записал два датасета (csv по остановкам и ЖК) в отдельные массивы python (один оказался с русской кодировкой, и пришлось воспользоваться модулем codecs).
После прогнал всё по циклу расчёта расстояния.
Далее, оставки, с удовлетворяющем условием (<= 1000метров) я записал в созданную мной БД (test.db) и после, сортировав её, экспортировал в общий csv файл и по каждому ЖК.
Работа заняла +-4 часа.
Время работы кода +- 15 секунд
