# загружаем библиотеки
from dash import Dash, dash_table, dcc, html
import dash
import dash_uploader as du
import pandas as pd
import db_operation as do
import plotly.io as pio
import dash_table
from dash.dependencies import Input, Output
import time
from multiprocessing import Manager, Process, Queue, Barrier, Lock
from datetime import datetime as dt
from queue import Empty
from dash.exceptions import PreventUpdate
import dash_bootstrap_components as dbc

# веб-приложение по адресу: http://127.0.0.1:8050/

""" Блок для определения взаимодествия с БД"""

do.delete_all_in_dir(dir = "all_data_for_save")

def update_db():

    global op_lst

    i = 0

    while 1:
        time.sleep(1)
        do.save_latest_file_to_db(schema = "public")
        i += 1

""" Делаем просмотр в браузере"""

pio.renderers.default = "browser"

""" Запускаем процессы """

if __name__ == '__main__':
    queue = Queue()
 
    # запускаем процесс сканирования папки all_data_for_save и добавления файлов в БД

    process1 = Process(target=update_db,     
                       args=())
    process1.start()

    app = dash.Dash(__name__, external_stylesheets=[dbc.themes.LUX])

    # Далее идет блок для работы веб-приложения

    # 1) Задаем путь для сохранения файлов
    du.configure_upload(app, r"all_data_for_save")

    # 2) Добавляем элементы веб-приложения
    app.layout = html.Div([

        html.Div([

            # задаем период обновления выпадающего списка
            dcc.Interval(
                id = "my_interval"
                , disabled = False
                , interval = 5*1000
                , n_intervals = 0
                , max_intervals = -1
            )

            # задаем выпадающий список
            , dcc.Dropdown(id = "table_dd",

                optionHeight = 35, # высота между значениями списка

                disabled = False, # включаем выпадающий список

                multi = False, # только одно выбранное значение

                searchable = True, # можно искать

                placeholder = "Please select...", # будет отображаться на самом выпадающем списке

                clearable = True, # можно очищать, нажав на крестик

                style = {"width": "50%", "margin-top": "15px", "margin-left": "15px"}, 

                persistence = True,
                persistence_type = "local" # запоминает, пока не удалятся куки в браузере
            )

        ])

        # Таблица
        , html.Div(id='figure'
            , style={"margin-top": "15px", "margin-bottom": "15px", "margin-left": "15px", "margin-right": "15px"}
            )

        # Область для загрузки
        , du.Upload()
    ])


    # Вызываем обновление списка таблиц
    @app.callback(
    Output("table_dd", "options"),
    [Input("my_interval", "n_intervals")]
    )

    def update_dd_opt(num):
        if num == 0:
            raise PreventUpdate
        else:
            return do.get_list_options(schema = "public")
        
    # Вызываем таблицу
    @app.callback(Output(component_id = "figure", component_property = "children"),
             [Input(component_id = "table_dd", component_property = "value")])
    def update_figure(table_dd):
        
        # Считываем таблицу 
        df_read = do.read_table(table_name = table_dd, schema = "public")

        # задаем параметры отображения таблицы
        return [dash_table.DataTable(
                data = df_read.to_dict("records")

                , columns = [
                    {"name": i, "id": i, "deletable": False, "selectable": False} 
                    for i in df_read.columns
                    ]
                , editable = False
                , filter_action = "native"
                , sort_action = "native"
                , sort_mode = "multi"
                , row_deletable = False
                , page_action = "none"
                , style_cell = {
                    "whitespace": "normal"
                    , 'minWidth': 120, 'maxWidth': 120, 'width': 120}
                , fixed_rows = {"headers": True, "data": 0}
                , virtualization = True
                , style_table = {'height': 500}
                , style_data={
                    'backgroundColor': 'white'
                }

                ,  style_data_conditional=[
                    {
                        'if': {'row_index': 'odd'},
                        'backgroundColor': 'rgb(245, 245, 245)',
                    }
                ]

                , style_header={
                    'backgroundColor': 'rgb(235, 235, 235)',
                    'fontWeight': 'bold'
                }
        )]

    # запускаем сервер
    app.run_server(debug=False)