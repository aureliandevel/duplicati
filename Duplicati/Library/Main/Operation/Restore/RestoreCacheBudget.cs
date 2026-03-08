// Copyright (C) 2025, The Duplicati Team
// https://duplicati.com, hello@duplicati.com
//
// Permission is hereby granted, free of charge, to any person obtaining a
// copy of this software and associated documentation files (the "Software"),
// to deal in the Software without restriction, including without limitation
// the rights to use, copy, modify, merge, publish, distribute, sublicense,
// and/or sell copies of the Software, and to permit persons to whom the
// Software is furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
// OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
// FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
// DEALINGS IN THE SOFTWARE.

namespace Duplicati.Library.Main.Operation.Restore
{
    /// <summary>
    /// Budget-allocation policy for restore cache components when an explicit restore cache size
    /// is configured. This split is not applied in unlimited mode.
    /// </summary>
    internal static class RestoreCacheBudget
    {
        /// <summary>
        /// Total number of slices in the restore cache budget split.
        /// </summary>
        private const int CACHE_BUDGET_DIVISOR = 10;

        /// <summary>
        /// Portion of the total restore cache budget reserved for the shared block store.
        /// </summary>
        public static long GetSharedBlockStoreBudget(long totalRestoreCacheBudget)
            => totalRestoreCacheBudget / CACHE_BUDGET_DIVISOR;

        /// <summary>
        /// Portion of the total restore cache budget reserved for the volume cache.
        /// </summary>
        public static long GetVolumeCacheBudget(long totalRestoreCacheBudget)
            => totalRestoreCacheBudget * (CACHE_BUDGET_DIVISOR - 1) / CACHE_BUDGET_DIVISOR;
    }
}