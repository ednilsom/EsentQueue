using System;
using System.IO;
using Microsoft.Isam.Esent.Interop;
using Microsoft.Isam.Esent.Interop.Vista;
using System.Xml.Serialization;

namespace EsentQueue
{
   /// <summary>
   /// A simple disk-backed queue using ManagedEsent
   /// </summary>
   public class PersistentQueue<T> : IDisposable
   {
      XmlSerializer _serializer = new XmlSerializer ( typeof ( T ) );

      private readonly QueueCursorCache _cursorCache;
      private readonly string _defaultName = "PersistentQueue";
      private readonly string _databaseName;
      private readonly Instance _instance;

      /// <summary>
      /// A simple disk-backed queue using ManagedEsent
      /// </summary>
      /// <param name="path">
      /// The path to the folder where all files will be stored.
      /// </param>
      /// <param name="startOption">
      /// The start option, Create New or Open Existing.
      /// </param>
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

      /// <summary>
      /// Gets the number of elements in the queue.
      /// </summary>
      /// <value>
      /// The number of elements contained in the queue.
      /// </value>
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

      /// <summary>
      /// Adds an object to the end of the Queue.
      /// </summary>
      /// <param name="item">
      /// The object to add to the Queue. It must be serializable
      /// </param>
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

      /// <summary>
      /// Removes and returns the object at the beginning of the Queue.
      /// </summary>
      /// <returns>
      /// The object that is removed from the beginning of the Queue
      /// </returns>
      public T Dequeue ( )
      {
         if ( !TryDequeue ( out T item ) )
         {
            throw new InvalidOperationException ( "No items in the queue." );
         }

         return item;
      }

      /// <summary>
      /// Removes the object at the beginning of the Queue, and copies it to the result parameter.
      /// </summary>
      /// <param name="item">
      /// The removed object.
      /// </param>
      /// <returns>
      /// true if the object is successfully removed; false if the Queue is empty
      /// </returns>
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

      /// <summary>
      /// Returns the object at the beginning of the Queue without removing it.
      /// </summary>
      /// <returns>
      /// The object at the beginning of the Queue.
      /// </returns>
      public T Peek ( )
      {
         if ( !TryPeek ( out T item ) )
         {
            throw new InvalidOperationException ( "No items in the queue." );
         }

         return item;
      }

      /// <summary>
      /// Returns a value that indicates whether there is an object at the beginning
      /// of the Queue, and if one is present, copies it to the result parameter.
      /// The object is not removed from the Queue.
      /// </summary>
      /// <param name="item">If present, the object at the beginning of the Queue;
      /// otherwise, the default value of T.</param>
      /// <returns>
      /// true if there is an object at the beginning of the Queue; false if the Queue is empty.
      /// </returns>
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

      /// <summary>
      /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
      /// </summary>
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

               string indexDef = "+Id\0\0";
               Api.JetCreateIndex ( session, tableid, "Primary", CreateIndexGrbit.IndexPrimary, indexDef, indexDef.Length, 100 );

               transaction.Commit ( CommitTransactionGrbit.None );
            }
         }
      }
   }
}
