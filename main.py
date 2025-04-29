import os
import logging

import sqlparse
import collections
import pandas as pd

logger = logging.getLogger(__name__)


def main():
    logging.basicConfig(level=logging.INFO,
                        format=u'%(filename)s:%(lineno)d #%(levelname)-8s [%(asctime)s] - %(name)s - %(message)s')
    logger.info("Скрипт запущен!")

    logger.info("Смотрю файлы в каталоге inputdata")
    for filename in os.listdir("inputdata"):
        if not filename.endswith(".sql"):
            logger.info(f"Пропускаю файл - {filename}. Нету расширения .sql")
            pass

        logger.info(f"Начинаю распарсивать {filename}")
        with open(f"inputdata/{filename}", "r", encoding="utf-8") as sqldump:
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

                contents[table_name].extend(
                    tuple(
                        s.value.strip('"\'')
                        for s in next(rec.get_sublists()).get_identifiers()
                    )
                    for rec in next(sublists).get_sublists()
                )

        data = {
            name: pd.DataFrame.from_records(table, columns=headers[name]) for name, table in contents.items()
        }

        for key, value in data.items():
            value.to_excel(f"outdata/{key}.xlsx", index=False)
            logger.info(f"Выгрузил данные в {key}.xlsx")

    logger.info("Закончил работу!")


if __name__ == "__main__":
    main()
