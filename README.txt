
DataBase Manager


How to use?
>> import config
>> from database_manager.core import DataBaseManager
>>
>> loader = DataBaseManager(config.db13)
>> loader.test_connection()
Connection is available!

Examples
>> loader.select("SELECT 1 FROM DUAL")
[(1,)]
