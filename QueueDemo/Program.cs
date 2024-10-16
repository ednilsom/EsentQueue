﻿namespace QueueDemo;

using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

using Collections.EsentQueue;

class Program
{
   private static readonly Random random = new ( );

   static void Main ( )
   {
      for ( int ItemCount = 50000; ItemCount < 300000; ItemCount += 50000 )
      {
         SingleThreadTest ( ItemCount );
         int workers = Environment.ProcessorCount;
         Console.WriteLine ( $"Using {workers} workers..." );
         MultiThreadReadTest ( ItemCount, workers );
         MultiThreadDequeueAndPeekTest ( ItemCount, workers, workers );
         Console.WriteLine ( );
      }
   }

   private static void MultiThreadReadTest ( int itemCount, int workers )
   {
      using EsentQueue<Event> queue = new ( "test", StartOption.OpenOrCreate );
      Stopwatch s = new ( );
      s.Start ( );
      for ( int i = 0; i < itemCount; i++ )
      {
         Event evt = CreateEvent ( );
         queue.Enqueue ( evt );
      }
      s.Stop ( );
      Console.WriteLine ( $"Added {itemCount} in {s.Elapsed}" );
      Console.WriteLine ( $"Count: {queue.Count}" );

      s.Restart ( );
      long count = 0;
      Task[] workerTasks = new Task[workers];
      for ( int i = 0; i < workers; i++ )
      {
         workerTasks[i] = Task.Run ( ( ) =>
           {
              try
              {
                 while ( queue.TryDequeue ( out Event item ) )
                 {
                    Interlocked.Increment ( ref count );
                 }
              }
              catch ( Exception )
              {
                 long c = Interlocked.Read ( ref count );
                 Console.WriteLine ( $"Failed at {c}" );
                 throw;
              }
           } );
      }
      Task.WaitAll ( workerTasks );
      s.Stop ( );
      Console.WriteLine ( $"Removed {itemCount} in {s.Elapsed}" );
      Console.WriteLine ( $"Count: {queue.Count}" );
   }

   private static void MultiThreadDequeueAndPeekTest ( int itemCount, int peekers, int dequeuers )
   {
      using EsentQueue<Event> queue = new ( "test", StartOption.OpenOrCreate );
      Stopwatch s = new ( );
      s.Start ( );
      for ( int i = 0; i < itemCount; i++ )
      {
         Event evt = CreateEvent ( );
         queue.Enqueue ( evt );
      }
      s.Stop ( );
      Console.WriteLine ( $"Added {itemCount} in {s.Elapsed}" );
      Console.WriteLine ( $"Count: {queue.Count}" );

      s.Restart ( );
      long count = 0, peekCount = 0;
      Event item;
      int total = dequeuers + peekers;
      Task[] workerTasks = new Task[total];
      for ( int i = 0; i < workerTasks.Length; i++ )
      {
         if ( i % 2 == 0 )
         {
            workerTasks[i] = Task.Run ( ( ) =>
              {
                 try
                 {
                    SpinWait spinWait = new ( );
                    while ( queue.TryPeek ( out item ) )
                    {
                       Interlocked.Increment ( ref peekCount );
                       spinWait.SpinOnce ( );
                    }
                 }
                 catch ( Exception )
                 {
                    long c = Interlocked.Read ( ref peekCount );
                    Console.WriteLine ( $"Failed at {c}" );
                    throw;
                 }
              } );
         }
         else
         {
            workerTasks[i] = Task.Run ( ( ) =>
              {
                 try
                 {
                    while ( queue.TryDequeue ( out item ) )
                    {
                       Interlocked.Increment ( ref count );
                    }
                 }
                 catch ( Exception )
                 {
                    long c = Interlocked.Read ( ref count );
                    Console.WriteLine ( $"Failed at {c}" );
                    throw;
                 }
              } );
         }
      }
      Task.WaitAll ( workerTasks );
      s.Stop ( );
      Console.WriteLine ( $"Removed {itemCount} in {s.Elapsed}" );
      Console.WriteLine ( $"Count: {queue.Count}" );
   }

   private static void SingleThreadTest ( int itemCount )
   {
      using EsentQueue<Event> queue = new ( "test", StartOption.CreateNew );
      Stopwatch s = new ( );
      s.Start ( );
      for ( int i = 0; i < itemCount; i++ )
      {
         Event evt = CreateEvent ( );
         queue.Enqueue ( evt );
      }
      s.Stop ( );
      Console.WriteLine ( $"Added {itemCount} in {s.Elapsed}" );
      Console.WriteLine ( $"Count: {queue.Count}" );

      s.Restart ( );
      while ( queue.TryDequeue ( out Event item ) )
      {
         // intentionally blank
      }
      s.Stop ( );
      Console.WriteLine ( $"Removed {itemCount} in {s.Elapsed}" );
      Console.WriteLine ( $"Count: {queue.Count}" );
   }

   private static Event CreateEvent ( )
   {
      return new Event ( )
      {
         MessageId = Guid.NewGuid ( ),
         Action = random.Next ( 0, 16 ),
         Type = random.Next ( 0, 16 ),
         ObjectId = Guid.NewGuid ( ),
         Timestamp = new DateTime ( 1000000000L + random.Next ( ) * 100L )
      };
   }
}

public struct Event
{
   public Guid MessageId;

   public int Action;

   public int Type;

   public Guid ObjectId;

   public DateTime Timestamp;
}
