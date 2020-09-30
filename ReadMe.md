HackerU Final Project
2020-09-16

# Описание программы
Программа по сбору статистических данных цен на квартиры в Москве с возможностью просмотра отчетов через браузер:
1. Квартиры, которые подешевели
1. Районы, которые дорожают
1. Новые квартиры

Далее приведены инструкции по установке, запуску программы и работе с ней. 
Все инструкции приведены для ОС WINDOWS.
ПО не тестировалось на MacOS.

# Установка
1. Создать папку на диске
1. Сохранить в нее файлы
1. Создать виртуальное окружение:    
    1. Запустить "Командная строка"    
    1. Перейти в рабочую папку (созданная на шаге 1)    
    1. Создать виртуальное окружение    
    '''
        python -m venv env    
    '''
    1. Активировать виртуальное окружение    
> env\Scripts\activate    
:exclamation: Все дальнейшие действия выполняются только в виртуальном окружении.    
1. Обновить pip install    
В командной строке выполнить:    
> python -m pip install --upgrade pip    
1. Установить BeautifulSoap    
В командной строке выполнить:    
> pip install BeautifulSoup4    
1. Установить SQLite3    
В командной строке выполнить:    
> pip install db-sqlite3    
1. Установить requests    
В командной строке выполнить:    
> pip install requests    
1. Установить PANDAS    
> pip install pandas    
1. Установить FLASK    
> pip install flask    
    
# Запуск программы    
При первом запуске программа создает неоьходимые для ее работы объекты базы данных.    
Программой не предусмотрена автоматическая загрузка данных. Загрузка данных инициируется пользователем.    
    
Чтобы запустить программу на выполнение, необходимо:    
1. В командой строке в рабочей директории выполнить    
> python app.py    
1. Последней строчкой программа выведет адрес для подключения:
> Running on http://127.0.0.1:5000/ (Press CTRL+C to quit)
1. Выполнить проверку подключения. Для этого перейти в браузере по адресу, указав после последнего слеша свое имя:
> http://127.0.0.1:5000/test/
например:
> http://127.0.0.1:5000/test/kate

    
# Работа программы через браузер    
## Описание возможных действий    
Программа позволяет выполнить следующие действия:    
1. Загрузка данных    
При загрузке данных анализируется, есть ли уже такой объект в базе.    
Если объект новый, то он сохраняется в базе.    
Если объект уже зарегистрирован в базе, то анализируется наличие изменения хотя бы в одном атрибуте, и в этом случае предыдущие значения объекта сохранюятся в архиве.    
Если же в системе есть объекты, по которым информация не получена, то объект считается удаленным из базы. При повторном получении объетка он будет считаться как новый.    
    
1. Просмотр отчетов    
В программе предусмотрено три отчета:    
    1. Квартиры, которые подешевели    
Выводится список объектов, цена которых уменьшилась относительно предыдущей загрузки.    
    1. Районы, которые подорожали    
Выводится список районов, итоговая сумма объектов которых изменилась в большую сторону.    
    1. Новые квартиры    
Выводится список объектов, впервые полученные при последней загрузке.    
    
## Загрузка данных    
Чтобы выполнить очередную загрузку данных необходимо:    
1. Открыть в браузере
1. 
 




