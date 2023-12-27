 internal class DatabaseHelper
    {
        private static string m_strMajorConnectionString = null;
        private static string m_strMinorConnectionString = null;

        public static string MajorConnectionString
        {
            get
            {
                if (string.IsNullOrEmpty(m_strMajorConnectionString))
                {
                    NpgsqlConnectionStringBuilder pgConnectStringBuilder = new NpgsqlConnectionStringBuilder
                    {
                        Host = "192.168.0.1",//主库
                        Port = 5432,
                        Username = "postgres",
                        Password = "123456",
                        Database = "test",
                        MinPoolSize = 50,
                        MaxPoolSize = 200
                    };
                    m_strMajorConnectionString = pgConnectStringBuilder.ConnectionString;
                }

                return m_strMajorConnectionString;
            }
        }

        public static string MinorConnectionString
        {
            get
            {
                if (string.IsNullOrEmpty(m_strMinorConnectionString))
                {
                    NpgsqlConnectionStringBuilder pgConnectStringBuilder = new NpgsqlConnectionStringBuilder
                    {
                        Host = "192.168.0.10",//从库
                        Port = 5432,
                        Username = "postgres",
                        Password = "123456",
                        Database = "test",
                        MinPoolSize = 50,
                        MaxPoolSize = 200
                    };
                    m_strMinorConnectionString = pgConnectStringBuilder.ConnectionString;
                }

                return m_strMinorConnectionString;
            }
        }
    }

        private static async Task<Test> GetTablesync(Envelope3D string strTableName, bool bFromMaster)
        {
            NpgsqlCommand _Command = null;
            NpgsqlConnection _Connection = null;
            NpgsqlDataReader _Reader = null;
            try
            {
                //PostgreSQL 不支持MARS，单个连接不能同时执行多个Command，并发时每次新建一个连接
                //连接数据库
                string strConnectionString = DatabaseHelper.MajorConnectionString;//主库
                if (!bFromMaster)
                    strConnectionString = DatabaseHelper.MinorConnectionString;//从库

                _Connection = new NpgsqlConnection(strConnectionString);
                await _Connection.OpenAsync();

                //SQL语句查询
                string sql = QueryTableSql(strTableName);

                //获取查询结果信息
                _Command = _Connection.CreateCommand();
                _Command.CommandText = sql;
                _Reader = await _Command.ExecuteReaderAsync();
                if (_Reader == null)
                {
                    await _Command.DisposeAsync();
                    await _Connection.CloseAsync();
                    return null;
                }

                Test ab = new Test();
                if (_Reader.Read())
                {
                    ab.A = _Reader.GetInt32(0);
                    ab.B = _Reader.GetDouble(1);
                    ab.C = _Reader.GetInt32(2);
                    ab.D = _Reader.GetDouble(3);
                    ab.E = _Reader.GetDouble(4);
                    ab.F = _Reader.GetDouble(5);
                    ab.G = _Reader.GetDouble(6);
                }
                return ab;
            }
            catch (Exception ex)
            {
                if (strTableName != "test")
                {
                    return await GetTablesync("test", true);
                }
                else 
                { 
                    return null;
                }
            }
            finally
            {
                //关闭数据库连接
                if (_Command != null) await _Command.DisposeAsync();
                if (_Connection != null) await _Connection.CloseAsync();
                if (_Reader != null) await _Reader.CloseAsync();
            }
        }
        public static Test[] GetTestABC(string strTableName, List<string> pStringSet, bool preCheck = true)
        {
            Test[] result = null;
            try
            {
                if (pStringSet == null || pStringSet.Count < 1)
                    return null;
                result = new Test[pStringSet.Count];
                Test test = GetTablesync(strTableName, true);//主库查询
                if (test == null)
                    test = GetTablesync(strTableName, false);//从库备份查询

                if (test == null)
                    return result;

                //划分多线程任务
                int nTaskCount = Environment.ProcessorCount;//服务器CPU数量
                List<List<Test>> taskTestSet = new List<List<Test>>();
                for (int i = 0; i < nTaskCount; i++)
                {
                    taskTestSet.Add(new List<Test>());
                }
                int index = 0;
                for (int i = 0; i < pStringSet.Count; i++)
                {
                    index = i % nTaskCount;
                    taskTestSet[index].Add(pStringSet[i]);
                }

                //启动各个任务
                List<Task> taskList = new List<Task>();
                for (int i = 0; i < nTaskCount; i++)
                {
                    if (taskTestSet[i].Count > 0)
                    {
                        Task task = IntersectionTask(test, taskTestSet[i], preCheck);
                        taskList.Add(task);
                    }
                }

                //等待任务全部结束
                Task.WaitAll(taskList.ToArray());

                return result;
            }
            catch (Exception ex)
            {
                return result;
            }
        }
