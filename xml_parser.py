import _csv
import csv
import multiprocessing
import os
from bs4 import BeautifulSoup as bs
import config

path_main = config.mapping_path
prefix = 'mp.prj_iedw_016.m_oakb_016_n'
files = list(filter(lambda x: prefix in x, os.listdir(path_main)))

ignore_fields = (
    'INFO_SYSTEM_ID',
    'DELETED_FLAG',
    'INPUT_FILE_ID',
    'WORKFLOW_RUN_ID',
    'ACTION_CD',
    'SESSION_INST_ID'
)

d_collector = {}
locker = multiprocessing.Lock()


class Connection:
    def __init__(self, name: str, to: str, soup: bs):
        try:
            print(f'{to + "&" + name}')
        except TypeError:
            print('Type error')
        self.to = to
        self.name_to = name
        self.soup = soup
        parser = self.soup.find('CONNECTOR', TOINSTANCE=to, TOFIELD=name)
        # REWRITE!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        if parser is None and to != 'Union' and 'EXP_' not in to:
            self.name_from, self.frm = None, None
        else:
            match to:
                case 'Union':  # Union case
                    names_to = list(map(
                        lambda x: x.get('INPUTFIELD'),
                        soup.find('TRANSFORMATION', NAME="Union").findAll('FIELDDEPENDENCY', OUTPUTFIELD=name)
                    ))
                    soups = list(map(
                        lambda x: self.soup.find('CONNECTOR', TOINSTANCE=to, TOFIELD=x), names_to
                    ))
                    self.name_from = list(map(lambda x: x.get('FROMFIELD'), soups))
                    self.frm = list(map(lambda x: x.get('FROMINSTANCE'), soups))
                case str() as t:
                    if 'EXP_' in t:  # EXP case
                        self.name_from = soup.find('TRANSFORMATION', NAME=t). \
                            find('TRANSFORMFIELD', NAME=name).get('EXPRESSION')

                        if self.name_from == name:
                            self.name_from = parser.get('FROMFIELD')
                            self.frm = parser.get('FROMINSTANCE')
                        else:
                            print(f'{self.name_from=}')
                            self.frm = None
                    else:  # default connector
                        self.name_from = parser.get('FROMFIELD')
                        self.frm = parser.get('FROMINSTANCE')


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
            i = 0
            self.d_write(col, i, c)
            while True:
                i += 1
                try:
                    c = Connection(self.bonds[f'{col}_{i - 1}']['name_from'],
                                   self.bonds[f'{col}_{i - 1}']['from'],
                                   self.soup)
                except TypeError:
                    break
                match c.name_from:
                    case list(l):  # union case
                        for j, l_ in enumerate(l):
                            k = i + 2
                            self.bonds[f'{col}_{j}_{i+1}'] = {
                                'name_from': l_,
                                'from': c.frm[j]
                            }
                            try:
                                c1 = Connection(l_,
                                                c.frm[j],
                                                self.soup)
                                self.d_write(col + f'_{j}', k, c1)
                            except Exception as ex:
                                print(f'{ex} occurred')
                                break
                            while True:
                                print(f'{j=}, {l_=}, {k=}')
                                k += 1
                                try:
                                    c1 = Connection(self.bonds[f'{col}_{j}_{k - 1}']['name_from'],
                                                    self.bonds[f'{col}_{j}_{k - 1}']['from'],
                                                    self.soup)
                                    self.d_write(col + f'_{j}', k, c1)
                                except Exception as ex:
                                    print(f'{ex} occurred')
                                    break
                        break
                    case str():
                        try:
                            self.d_write(col, i, c)
                        except Exception as ex:
                            print(f'{ex} occurred')
                            break
                    case None:
                        break

    def d_write(self, col: str, i: int, c: Connection) -> None:
        if c.name_from is None:
            raise AttributeError('name_from is None')
        else:
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
    n_view = NView(b_s, len(b_s.findAll('INSTANCE')))
    n_view.d_fill()
    return n_view


def view_writer(view: NView) -> list[tuple]:
    "------------------------------Rewrite----------------------------------------------"
    res_list = []
    for col in view.cols:
        res_list.append(
            (view.bonds[f'{col}_{0}']['connector'].to,) + tuple(
                view.bonds[k]['from'] for k in view.bonds.keys() if col in k
            )
        )  # source name
        res_list.append(
            (col,) + tuple(
                view.bonds[k]['name_from'] for k in view.bonds.keys() if col in k
            )
        )  # source col name
    "--------------------------------------------------------------------------------------"
    return res_list


def res_collector(_file: str) -> list[list[tuple]]:
    """Multiprocess collecting parsed xml"""
    res = parse(_file)
    r_list = view_writer(res)
    d_collector[_file] = list(map(lambda x: (_file,) + x, r_list))
    print(_file, '\n', list(map(lambda x: (_file,) + x, r_list)))


def post_file() -> None:
    """csv writing"""
    # print(response)
    with open('n_connections.csv', encoding='utf-8', mode='w') as csvfile:
        writer = csv.writer(csvfile, delimiter='\t')
        for k in d_collector.values():
            for r in k:
                writer.writerow(r)
                # for r1 in r:
                #     try:
                #         writer.writerow(r1)
                #     except _csv.Error:
                #         continue


if __name__ == '__main__':
    for file in files:
        res_collector(file)
    post_file()
    # with open('n_connections.csv', encoding='utf-8', mode='w') as csvfile:
    #     "-------------------------Rewrite-----------------------------------"
    #     writer = csv.writer(csvfile, delimiter='\t')
    #     for file in files:
    #         # res = multiprocessing.Process(target=parse, args=(file,))
    #         res = parse(file)
    #         r_list = list(map(lambda x: (file,) + x, view_writer(res)))
    #         for r in r_list:
    #             writer.writerow(r)
    "-------------------------------------------------------------------"
    # with multiprocessing.Pool(multiprocessing.cpu_count() * 3) as pool:
    #     pool.map_async(res_collector, [file for file in files], callback=post_file)
    #     pool.close()
    #     pool.join()
