namespace EsentQueue;

using System.Collections.Concurrent;

using Microsoft.Isam.Esent.Interop;

internal class QueueCursorCache ( Instance instance, string databaseName )
{
   private readonly ConcurrentBag<QueueCursor> _cursors = [];

   public QueueCursor GetCursor ( )
   {
      if ( !_cursors.TryTake ( out QueueCursor cursor ) )
      {
         cursor = new QueueCursor ( instance, databaseName );
      }

      return cursor;
   }

   public void Return ( QueueCursor cursor )
   {
      _cursors.Add ( cursor );
   }

   public void FreeAll ( )
   {
      while ( _cursors.TryTake ( out QueueCursor cursor ) )
      {
         cursor.Dispose ( );
      }
   }
}
