using System;
using Microsoft.Isam.Esent.Interop;

namespace EsentQueue
{
   internal class QueueCursor : IDisposable
   {
      private readonly Instance _instance;
      private readonly string _databaseName;
      private readonly Session _session;
      private JET_DBID _dbId;
      private readonly Table _dataTable;
      private readonly JET_COLUMNID _serializedObjectCol, _messageIdCol;

      public QueueCursor ( Instance instance, string databaseName )
      {
         _instance = instance;
         _databaseName = databaseName;
         _session = new Session ( _instance );
         Api.JetOpenDatabase ( _session, _databaseName, null, out _dbId, OpenDatabaseGrbit.None );
         _dataTable = new Table ( _session, _dbId, "Data", OpenTableGrbit.None );
         _messageIdCol = Api.GetTableColumnid ( _session, _dataTable, "Id" );
         _serializedObjectCol = Api.GetTableColumnid ( _session, _dataTable, "SerializedObject" );
      }

      public Session Session => _session;

      public Table DataTable => _dataTable;

      public JET_COLUMNID MessageIdColumn => _messageIdCol;

      public JET_COLUMNID SerializedObjectColumn => _serializedObjectCol;

      public Transaction BeginTransaction ( )
      {
         return new Transaction ( _session );
      }

      public Update CreateDataTableUpdate ( JET_prep prep = JET_prep.Insert )
      {
         return new Update ( _session, _dataTable, prep );
      }

      public void Dispose ( )
      {
         _dataTable?.Dispose ( );
         _session?.Dispose ( );
      }
   }
}
