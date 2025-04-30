import os
import logging
from pathlib import Path

import sqlparse
import collections
import pandas as pd

logger = logging.getLogger(__name__)


def main():
    logging.basicConfig(level=logging.INFO,
                        format=u'%(filename)s:%(lineno)d #%(levelname)-8s [%(asctime)s] - %(name)s - %(message)s')
    logger.info("Скрипт запущен!")

    inputdata_path = Path(os.path.abspath("inputdata"))
    inputdata_path.mkdir(parents=True, exist_ok=True)

    logger.info("Смотрю файлы в каталоге inputdata")
    for filename in os.listdir(inputdata_path):
        if not filename.endswith(".sql"):
            logger.info(f"Пропускаю файл - {filename}. Нету расширения .sql")
            pass

        logger.info(f"Начинаю распарсивать {filename}")
        with open(f"inputdata/{filename}", "r") as sqldump:
            parser = sqlparse.parsestream(sqldump)
            headers = {}
            contents = collections.defaultdict(list)

            for statement in parser:
                if statement.get_type() == 'INSERT':
                    sublists = statement.get_sublists()
                    table_info = next(sublists)
                    table_name = table_info.get_name()

                headers[table_name] = [
                    col.get_name()
                    for col in table_info.get_parameters()
                ]

                try:
                    value_block = next(sublists)
                    for rec in value_block.get_sublists():
                        try:
                            inner = next(rec.get_sublists())
                            row = tuple(
                                s.value.strip('"\'') for s in inner.get_identifiers()
                            )
                            contents[table_name].append(row)

                        except StopIteration:
                            logger.warning(f"Пустой подсписок в INSERT в таблицу {table_name}, пропускаю строку.")

                        except Exception as e:
                            logger.error(f"Ошибка при разборе строки INSERT: {e}")

                except StopIteration:
                    logger.warning(f"Не удалось получить VALUES-блок для {table_name}, пропускаю.")

                except Exception as e:
                    logger.error(f"Ошибка при разборе VALUES блока в INSERT: {e}")

        data = {
            name: pd.DataFrame.from_records(table, columns=headers[name]) for name, table in contents.items()
        }

        for key, value in data.items():
            outdata_filepath = Path(os.path.abspath(f"outdata/{key}.xlsx"))
            outdata_filepath.parent.mkdir(parents=True, exist_ok=True)
            value.to_excel(outdata_filepath, index=False)
            logger.info(f"Выгрузил данные в {key}.xlsx")

    logger.info("Закончил работу!")


if __name__ == "__main__":
    main()
