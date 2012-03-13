using System.Runtime.InteropServices;

namespace RavenFS.Rdc.Wrapper
{
	[StructLayout(LayoutKind.Sequential, Pack = 4)]
	public struct SimilarityDumpData
	{
		public uint FileIndex;
		public SimilarityData Data;
	}
}