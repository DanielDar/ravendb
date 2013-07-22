﻿using System;
using System.IO;
using System.Runtime.InteropServices;
using System.Security.Permissions;
using RavenFS.Synchronization.Rdc.Wrapper.Unmanaged;

namespace RavenFS.Synchronization.Rdc.Wrapper
{
	[ClassInterface(ClassInterfaceType.None)]
	[Guid("96236A82-8DBC-11DA-9E3F-0011114AE311")]
	[SecurityPermission(SecurityAction.Demand, UnmanagedCode = true)]
	public class RdcFileWriter : IRdcFileWriter
	{
		private readonly Stream _stream;

		public RdcFileWriter(Stream stream)
		{
			_stream = stream;
		}

		public void Write(UInt64 offsetFileStart, uint bytesToWrite, ref IntPtr buffer)
		{
			var outBuff = new Byte[bytesToWrite];

			Marshal.Copy(buffer, outBuff, 0, (int) bytesToWrite);

			_stream.Seek((long) offsetFileStart, SeekOrigin.Begin);
			_stream.Write(outBuff, 0, (int) bytesToWrite);
		}

		public void Truncate()
		{
		}

		public void DeleteOnClose()
		{
		}
	}
}