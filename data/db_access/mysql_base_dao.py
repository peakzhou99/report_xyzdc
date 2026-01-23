from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker


class MySQLDao:
    def __init__(self):
        self.engine = create_engine("mysql+pymysql://llmmodel:llmmodelNsRMSKC1@10.10.39.86:3306/finchinadb")
        self.Session = sessionmaker(bind=self.engine)
        self.table_name = self.__class__.__name__.replace("Dao", "").lower()

    def get_session(self):
        return self.Session()

    def select_one_by_sql(self, query, params=None, return_type=None):
        result = None
        session = self.get_session()
        try:
            _result = session.execute(text(query), params).mappings().fetchall()
            session.commit()
            _result = self.convert_result_type(_result, return_type)
            if isinstance(_result, list) and len(_result) > 0:
                result = _result[0]
        except Exception as e:
            session.rollback()
        finally:
            session.close()
        return result

    def select_list_by_sql(self, query, params=None, return_type=None):
        _result = None
        session = self.get_session()
        try:
            __result = session.execute(text(query), params).mappings().fetchall()
            session.commit()
            _result = self.convert_result_type(__result, return_type)
        except Exception as e:
            print(e)
            session.rollback()
        finally:
            session.close()
        return _result

    def convert_result_type(self, results, return_type):
        _mapped_results = []
        if return_type is not None:
            _column_mappings = {col.name.upper(): attr.key for col, attr in zip(return_type.__mapper__.columns, return_type.__mapper__.attrs)}
            for _item in results:
                if not isinstance(_item, return_type):
                    _data = {_column_mappings[key]: value for key, value in _item.items() if key in _column_mappings.keys()}
                    _mapped_results.append(return_type(**_data))
        else:
            _mapped_results = results
        return _mapped_results

    def count(self):
        session = self.get_session()
        try:
            query = f"select count(1) from {self.table_name}"
            result = session.execute(text(query)).fetchall()
            session.commit()
        except Exception as e:
            session.rollback()
        finally:
            session.close()