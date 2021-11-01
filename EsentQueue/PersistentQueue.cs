using System;
using System.IO;
using Microsoft.Isam.Esent.Interop;
using Microsoft.Isam.Esent.Interop.Vista;
using System.Xml.Serialization;

namespace EsentQueue
{
   public class PersistentQueue<T> : IDisposable
   {
      XmlSerializer _serializer = new XmlSerializer ( typeof ( T ) );

      private readonly QueueCursorCache _cursorCache;
      private readonly string _defaultName = "PersistentQueue";
      private readonly string _databaseName;
      private readonly Instance _instance;

      public PersistentQueue ( string path, StartOption startOption )
      {
         _instance = new Instance ( "EsentQueue.PersistentQueue", _defaultName );
         _instance.Parameters.CreatePathIfNotExist = true;
         _instance.Parameters.CircularLog = true;
         _instance.Parameters.MaxVerPages = 256;
         _instance.Parameters.LogFileDirectory = path;
         _instance.Parameters.SystemDirectory = path;
         _instance.Parameters.TempDirectory = path;
         _instance.Parameters.AlternateDatabaseRecoveryDirectory = path;
         _instance.Init ( );
         _databaseName = Path.Combine ( path, _defaultName + ".edb" );
         _cursorCache = new QueueCursorCache ( _instance, _databaseName );

         if ( !File.Exists ( _databaseName ) || startOption == StartOption.CreateNew )
         {
            CreateDatabase ( );
         }
         else
         {
            CheckDatabase ( );
         }
      }

      public int Count
      {
         get
         {
            var cursor = _cursorCache.GetCursor ( );
            try
            {
               int count = 0;
               Api.MoveBeforeFirst ( cursor.Session, cursor.DataTable );
               while ( Api.TryMoveNext ( cursor.Session, cursor.DataTable ) )
               {
                  count++;
               }

               return count;
            }
            finally
            {
               _cursorCache.Return ( cursor );
            }
         }
      }

      public void Enqueue ( T item )
      {
         var cursor = _cursorCache.GetCursor ( );
         try
         {
            lock ( _instance )
            {
               using ( var transaction = cursor.BeginTransaction ( ) )
               {
                  using ( var update = cursor.CreateDataTableUpdate ( ) )
                  {
                     using ( var colStream = new ColumnStream ( cursor.Session, cursor.DataTable, cursor.SerializedObjectColumn ) )
                     {
                        _serializer.Serialize ( colStream, item );
                     }

                     update.Save ( );
                  }

                  transaction.Commit ( CommitTransactionGrbit.LazyFlush );
               }
            }
         }
         finally
         {
            _cursorCache.Return ( cursor );
         }
      }

      public T Dequeue ( )
      {
         if ( !TryDequeue ( out T item ) )
         {
            throw new InvalidOperationException ( "No items in the queue." );
         }

         return item;
      }

      public bool TryDequeue ( out T item )
      {
         var cursor = _cursorCache.GetCursor ( );
         try
         {
            lock ( _instance )
            {
               using ( var tx = cursor.BeginTransaction ( ) )
               {
                  if ( !Api.TryMoveFirst ( cursor.Session, cursor.DataTable ) )
                  {
                     item = default;
                     return false;
                  }

                  using ( var colStream = new ColumnStream ( cursor.Session, cursor.DataTable, cursor.SerializedObjectColumn ) )
                  {
                     item = (T)_serializer.Deserialize ( colStream );
                  }

                  Api.JetDelete ( cursor.Session, cursor.DataTable );
                  tx.Commit ( CommitTransactionGrbit.LazyFlush );
                  return true;
               }
            }
         }
         finally
         {
            _cursorCache.Return ( cursor );
         }
      }

      public T Peek ( )
      {
         if ( !TryPeek ( out T item ) )
         {
            throw new InvalidOperationException ( "No items in the queue." );
         }

         return item;
      }

      public bool TryPeek ( out T item )
      {
         var cursor = _cursorCache.GetCursor ( );
         try
         {
            lock ( _instance )
            {
               using ( var tx = cursor.BeginTransaction ( ) )
               {
                  if ( !Api.TryMoveFirst ( cursor.Session, cursor.DataTable ) )
                  {
                     item = default;
                     return false;
                  }

                  using ( var colStream = new ColumnStream ( cursor.Session, cursor.DataTable, cursor.SerializedObjectColumn ) )
                  {
                     item = (T)_serializer.Deserialize ( colStream );
                  }

                  return true;
               }
            }
         }
         finally
         {
            _cursorCache.Return ( cursor );
         }
      }

      public void Dispose ( )
      {
         _cursorCache?.FreeAll ( );
         _instance?.Dispose ( );
         GC.SuppressFinalize ( this );
      }

      private void CheckDatabase ( )
      {
         // For now assume everything is correct. just attach the db.
         using ( var session = new Session ( _instance ) )
         {
            Api.JetAttachDatabase ( session, _databaseName, AttachDatabaseGrbit.None );
         }
      }

      private void CreateDatabase ( )
      {
         using ( var session = new Session ( _instance ) )
         {

            Api.JetCreateDatabase ( session, _databaseName, null, out JET_DBID dbid, CreateDatabaseGrbit.OverwriteExisting );

            JET_COLUMNDEF colDef;

            using ( var transaction = new Transaction ( session ) )
            {
               Api.JetCreateTable ( session, dbid, "Data", 16, 100, out JET_TABLEID tableid );

               colDef = new JET_COLUMNDEF ( )
               {
                  coltyp = VistaColtyp.LongLong,
                  grbit = ColumndefGrbit.ColumnNotNULL | ColumndefGrbit.ColumnAutoincrement
               };

               Api.JetAddColumn ( session, tableid, "Id", colDef, null, 0, out JET_COLUMNID colid );

               colDef = new JET_COLUMNDEF ( )
               {
                  coltyp = JET_coltyp.LongBinary,
               };
               Api.JetAddColumn ( session, tableid, "SerializedObject", colDef, null, 0, out colid );

               string indexDef;
               indexDef = "+Id\0\0";
               Api.JetCreateIndex ( session, tableid, "primary", CreateIndexGrbit.IndexPrimary, indexDef, indexDef.Length, 100 );

               transaction.Commit ( CommitTransactionGrbit.None );
            }
         }
      }
   }
}
