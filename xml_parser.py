import csv
import os
from bs4 import BeautifulSoup as bs
import config

path_main = config.mapping_path
files = os.listdir(path_main)

ignore_fields = (
    'INFO_SYSTEM_ID',
    'DELETED_FLAG',
    'INPUT_FILE_ID',
    'WORKFLOW_RUN_ID',
    'ACTION_CD'
)

exp_fields = (
    'EXP_FIRST',
    'EXP_LAST'
)


class Connection:
    def __init__(self, name: str, to: str, soup: bs):
        self.to = to
        self.name_to = name
        self.soup = soup
        parser = self.soup.find('CONNECTOR', TOINSTANCE=to, TOFIELD=name)
        if to in exp_fields:
            self.name_from = soup.find('TRANSFORMATION', NAME=to).find('TRANSFORMFIELD', NAME=name).get('EXPRESSION')
        else:
            try:
                self.name_from = parser.get('FROMFIELD')
            except AttributeError:
                self.name_from = None
        try:
            self.frm = parser.get('FROMINSTANCE')
        except AttributeError:
            self.frm = None


class NView:
    def __init__(self, soup: bs, n_inst: int):
        self.n_inst = n_inst
        self.soup = soup
        parser = self.soup.find('INSTANCE', TRANSFORMATION_TYPE="Target Definition", TYPE="TARGET")
        self.name = parser.get('TRANSFORMATION_NAME')
        self.alias = parser.get('NAME')
        self.cols = list(filter(
            lambda x: x not in ignore_fields,
            map(
                lambda x: x.get('TOFIELD'),
                self.soup.findAll('CONNECTOR', TOINSTANCE=self.alias, TOINSTANCETYPE="Target Definition")
            )))
        self.bonds = {}

    def d_fill(self) -> None:
        """fill 'bonds' dict with parsed columns connections"""
        for col in self.cols:
            c = Connection(col, self.alias, self.soup)
            self.d_write(col, 0, c)
            for i in range(1, self.n_inst):
                c = Connection(self.bonds[f'{col}_{i - 1}']['name_from'],
                               self.bonds[f'{col}_{i - 1}']['from'],
                               self.soup)
                self.d_write(col, i, c)

    def d_write(self, col: str, i: int, c: Connection) -> None:
        self.bonds[f'{col}_{i}'] = {
            'connector': c,
            'name_from': c.name_from,
            'from': c.frm
        }


def file_reader(_file: str) -> bs:
    with open(path_main + '\\' + _file, mode='r', encoding='utf-8') as f:
        lines = f.readlines()
    return bs(''.join(lines), "xml")


def parse(_file: str) -> NView:
    b_s = file_reader(_file)
    # c_tables = [(tbl.get('TRANSFORMATION_NAME'), tbl.get('NAME')) for tbl in
    #             b_s.findAll('INSTANCE', TRANSFORMATION_TYPE="Source Definition", TYPE="SOURCE")]

    n_view = NView(b_s, len(b_s.findAll('INSTANCE')))
    n_view.d_fill()

    # rows = b_s.findAll('TABLEATTRIBUTE', NAME='Sql Query')
    # rows = ';'.join((row.get('VALUE') for row in rows))
    # rows = sp.format(rows, reindent=True, keyword_case='upper')
    return n_view


def view_writer(view: NView) -> list[tuple]:
    print(view.name)
    res_list = [(view.name,) + tuple(
        view.bonds[f'{view.cols[0]}_{i}']['from'] for i in range(view.n_inst)
    )] + [
                   (col,) + tuple(
                       view.bonds[f'{col}_{i}']['name_from'] for i in range(view.n_inst)
                   )
                   for col in view.cols
               ]
    res_list = list(map(lambda x: (view.name,) + x, res_list))
    return res_list


if __name__ == '__main__':
    with open('n_connections.csv', encoding='utf-8', mode='w') as csvfile:
        "-------------------------Rewrite-----------------------------------"
        writer = csv.writer(csvfile, delimiter='\t')
        for file in files:
            # res = multiprocessing.Process(target=parse, args=(file,))
            res = parse(file)
            r_list = view_writer(res)
            for r in r_list:
                writer.writerow(r)
        "-------------------------------------------------------------------"
