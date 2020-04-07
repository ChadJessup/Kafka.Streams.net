//namespace Kafka.Streams.Tests.State.Internals
//{
//    /*






//    *

//    *





//    */















































//    /**
//     * The purpose of this test is, to catch interface changes if we upgrade {@link RocksDB}.
//     * Using reflections, we make sure the {@link RocksDBGenericOptionsToDbOptionsColumnFamilyOptionsAdapter} maps all
//     * methods from {@link DBOptions} and {@link ColumnFamilyOptions} to/from {@link Options} correctly.
//     */

//    public class RocksDBGenericOptionsToDbOptionsColumnFamilyOptionsAdapterTest
//    {
//        private List<string> ignoreMethods = new LinkedList<string>() {
//        {
//            add("isOwningHandle");
//        add("dispose");
//        add("wait");
//        add("equals");
//        add("getClass");
//        add("hashCode");
//        add("notify");
//        add("notifyAll");
//        add("toString");
//    }
//    };

    
//    private DBOptions dbOptions;

//    private ColumnFamilyOptions columnFamilyOptions;

//    [Fact]
//    public void ShouldOverwriteAllOptionsMethods()
//    {// throws Exception
//        foreach (Method method in Options.getMethods())
//        {
//            if (!ignoreMethods.Contains(method.getName()))
//            {
//                RocksDBGenericOptionsToDbOptionsColumnFamilyOptionsAdapter
//                    .getDeclaredMethod(method.getName(), method.getParameterTypes());
//            }
//        }
//    }

//    [Fact]
//    public void ShouldForwardAllDbOptionsCalls()
//    {// throws Exception
//        foreach (Method method in Options.getMethods())
//        {
//            if (!ignoreMethods.Contains(method.getName()))
//            {
//                try
//                {
//                    DBOptions.getMethod(method.getName(), method.getParameterTypes());
//                    verifyDBOptionsMethodCall(method);
//                }
//                catch (NoSuchMethodException expectedAndSwallow) { }
//            }
//        }
//    }

//    private void VerifyDBOptionsMethodCall(Method method)
//    {// throws Exception
//        RocksDBGenericOptionsToDbOptionsColumnFamilyOptionsAdapter optionsFacadeDbOptions
//            = new RocksDBGenericOptionsToDbOptionsColumnFamilyOptionsAdapter(dbOptions, new ColumnFamilyOptions());

//        object[] parameters = getDBOptionsParameters(method.getParameterTypes());

//        try
//        {
//            reset(dbOptions);
//            replay(dbOptions);
//            method.invoke(optionsFacadeDbOptions, parameters);
//            verify();
//            Assert.True(false, "Should have called DBOptions." + method.getName() + "()");
//        }
//        catch (InvocationTargetException undeclaredMockMethodCall)
//        {
//            Assert.Equal(undeclaredMockMethodCall.getCause(), instanceOf(AssertionError));
//            Assert.Equal(undeclaredMockMethodCall.getCause().getMessage().trim(),
//                matchesPattern("Unexpected method call DBOptions\\." + method.getName() + "((.*\n*)*):"));
//        }
//    }

//    private object[] GetDBOptionsParameters(Class<?>[] parameterTypes)
//    {// throws Exception
//        object[] parameters = new object[parameterTypes.Length];

//        for (int i = 0; i < parameterTypes.Length; ++i)
//        {
//            switch (parameterTypes[i].getName())
//            {
//                case "bool":
//                    parameters[i] = true;
//                    break;
//                case "int":
//                    parameters[i] = 0;
//                    break;
//                case "long":
//                    parameters[i] = 0L;
//                    break;
//                case "java.util.Collection":
//                    parameters[i] = new ArrayList<>();
//                    break;
//                case "org.rocksdb.AccessHint":
//                    parameters[i] = AccessHint.NONE;
//                    break;
//                case "org.rocksdb.Cache":
//                    parameters[i] = new LRUCache(1L);
//                    break;
//                case "org.rocksdb.Env":
//                    parameters[i] = Env.getDefault();
//                    break;
//                case "org.rocksdb.InfoLogLevel":
//                    parameters[i] = InfoLogLevel.FATAL_LEVEL;
//                    break;
//                case "org.rocksdb.Logger":
//                    parameters[i] = new Logger(new Options())
//                    {


//                        protected void log(InfoLogLevel infoLogLevel, string logMsg) { }
//            };
//            break;
//                case "org.rocksdb.RateLimiter":
//                    parameters[i] = new RateLimiter(1L);
//            break;
//                case "org.rocksdb.SstFileManager":
//                    parameters[i] = new SstFileManager(Env.getDefault());
//            break;
//                case "org.rocksdb.WALRecoveryMode":
//                    parameters[i] = WALRecoveryMode.AbsoluteConsistency;
//            break;
//                case "org.rocksdb.WriteBufferManager":
//                    parameters[i] = new WriteBufferManager(1L, new LRUCache(1L));
//            break;
//            default:
//                    parameters[i] = parameterTypes[i].newInstance();
//        }
//    }

//        return parameters;
//    }

//    [Fact]
//    public void ShouldForwardAllColumnFamilyCalls()
//    {// throws Exception
//        foreach (Method method in Options.getMethods())
//        {
//            if (!ignoreMethods.Contains(method.getName()))
//            {
//                try
//                {
//                    ColumnFamilyOptions.getMethod(method.getName(), method.getParameterTypes());
//                    verifyColumnFamilyOptionsMethodCall(method);
//                }
//                catch (NoSuchMethodException expectedAndSwallow) { }
//            }
//        }
//    }

//    private void VerifyColumnFamilyOptionsMethodCall(Method method)
//    {// throws Exception
//        RocksDBGenericOptionsToDbOptionsColumnFamilyOptionsAdapter optionsFacadeColumnFamilyOptions
//            = new RocksDBGenericOptionsToDbOptionsColumnFamilyOptionsAdapter(new DBOptions(), columnFamilyOptions);

//        object[] parameters = getColumnFamilyOptionsParameters(method.getParameterTypes());

//        try
//        {
//            reset(columnFamilyOptions);
//            replay(columnFamilyOptions);
//            method.invoke(optionsFacadeColumnFamilyOptions, parameters);
//            verify();
//            Assert.True(false, "Should have called ColumnFamilyOptions." + method.getName() + "()");
//        }
//        catch (InvocationTargetException undeclaredMockMethodCall)
//        {
//            Assert.Equal(undeclaredMockMethodCall.getCause(), instanceOf(AssertionError));
//            Assert.Equal(undeclaredMockMethodCall.getCause().getMessage().trim(),
//                matchesPattern("Unexpected method call ColumnFamilyOptions\\." + method.getName() + "(.*)"));
//        }
//    }

//    private object[] GetColumnFamilyOptionsParameters(Class<?>[] parameterTypes)
//    {// throws Exception
//        object[] parameters = new object[parameterTypes.Length];

//        for (int i = 0; i < parameterTypes.Length; ++i)
//        {
//            switch (parameterTypes[i].getName())
//            {
//                case "bool":
//                    parameters[i] = true;
//                    break;
//                case "double":
//                    parameters[i] = 0.0d;
//                    break;
//                case "int":
//                    parameters[i] = 0;
//                    break;
//                case "long":
//                    parameters[i] = 0L;
//                    break;
//                case "[I":
//                    parameters[i] = System.Array.Empty<int>();
//                    break;
//                case "java.util.List":
//                    parameters[i] = new ArrayList<>();
//                    break;
//                case "org.rocksdb.AbstractCompactionFilter":
//                    parameters[i] = new RemoveEmptyValueCompactionFilter();
//                    break;
//                case "org.rocksdb.AbstractCompactionFilterFactory":
//                    parameters[i] = new AbstractCompactionFilterFactory()
//                    {



//                        public AbstractCompactionFilter<?> createCompactionFilter(Context context)
//                    {
//                        return null;
//                    }


//                    public string name()
//                    {
//                        return "AbstractCompactionFilterFactory";
//                    }
//            };
//            break;
//                case "org.rocksdb.AbstractComparator":
//                    parameters[i] = new BytewiseComparator(new ComparatorOptions());
//            break;
//                case "org.rocksdb.BuiltinComparator":
//                    parameters[i] = BuiltinComparator.BYTEWISE_COMPARATOR;
//            break;
//                case "org.rocksdb.CompactionPriority":
//                    parameters[i] = CompactionPriority.ByCompensatedSize;
//            break;
//                case "org.rocksdb.CompactionStyle":
//                    parameters[i] = CompactionStyle.UNIVERSAL;
//            break;
//                case "org.rocksdb.CompressionType":
//                    parameters[i] = CompressionType.NO_COMPRESSION;
//            break;
//                case "org.rocksdb.MemTableConfig":
//                    parameters[i] = new VectorMemTableConfig();
//            break;
//                case "org.rocksdb.MergeOperator":
//                    parameters[i] = new StringAppendOperator();
//            break;
//                case "org.rocksdb.TableFormatConfig":
//                    parameters[i] = new PlainTableConfig();
//            break;
//            default:
//                    parameters[i] = parameterTypes[i].newInstance();
//        }
//    }

//        return parameters;
//    }
//}
//}
///*






//*

//*





//*/















































///**
// * The purpose of this test is, to catch interface changes if we upgrade {@link RocksDB}.
// * Using reflections, we make sure the {@link RocksDBGenericOptionsToDbOptionsColumnFamilyOptionsAdapter} maps all
// * methods from {@link DBOptions} and {@link ColumnFamilyOptions} to/from {@link Options} correctly.
// */











