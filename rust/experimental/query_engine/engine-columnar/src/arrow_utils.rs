// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use arrow::{array::*, buffer::*, datatypes::*};

pub unsafe fn ensure_nulls_unchecked(
    nulls: &mut Option<MutableBuffer>,
    bit_length: usize,
    fill_count: usize,
) {
    if nulls.is_none() {
        let mut buffer = MutableBuffer::new_null(bit_length);
        unsafe { fill_bits_from_start_unchecked(&mut buffer, fill_count) };
        *nulls = Some(buffer);
    }
}

pub unsafe fn fill_bits_from_start_unchecked(buffer: &mut MutableBuffer, count: usize) {
    debug_assert!(count <= buffer.len() * 8);

    if count == 0 {
        return;
    }

    let full_bytes = count / 8;
    let remainder_bits = count % 8;

    // 1. Fill all completely filled bytes with 1s
    if full_bytes > 0 {
        buffer[0..full_bytes].fill(0xFF);
    }

    // 2. Set only the targeted lower bits (LSB) in the final partial byte
    if remainder_bits > 0 {
        let mask = (1 << remainder_bits) - 1;
        buffer[full_bytes] |= mask;
    }
}

pub unsafe fn fill_bit_range_unchecked(
    buffer: &mut [u8],
    start_bit_inclusive: usize,
    end_bit_exclusive: usize,
) {
    debug_assert!(start_bit_inclusive <= end_bit_exclusive);
    debug_assert!(end_bit_exclusive <= buffer.len() * 8);

    if start_bit_inclusive == end_bit_exclusive {
        return;
    }

    let start_byte = start_bit_inclusive / 8;
    let end_byte = (end_bit_exclusive - 1) / 8;

    let start_bit = start_bit_inclusive % 8;
    let end_bit = (end_bit_exclusive - 1) % 8;

    if start_byte == end_byte {
        let mask = (((1u16 << (end_bit - start_bit + 1)) - 1) << start_bit) as u8;
        buffer[start_byte] |= mask;
        return;
    }

    // First partial byte (LSB)
    buffer[start_byte] |= !0u8 << start_bit;

    // Full bytes in the middle
    buffer[start_byte + 1..end_byte].fill(0xff);

    // Last partial byte (LSB)
    buffer[end_byte] |= (!0u8) >> (7 - end_bit);
}

pub unsafe fn fill_from_slice_unchecked(
    source: &[u8],
    destination: &mut [u8],
    destination_offset: usize,
    count: usize,
) {
    unsafe {
        let src = source.as_ptr();
        let dst = destination.as_mut_ptr().add(destination_offset);
        std::ptr::copy_nonoverlapping(src, dst, count)
    }
}

pub trait ArrowArrayValueAccessor<V> {
    fn get_value_null_safe(&self, key_index: usize) -> Option<V>;
}

impl<T: Array + ArrayAccessor> ArrowArrayValueAccessor<T::Item> for T {
    fn get_value_null_safe(&self, key_index: usize) -> Option<T::Item> {
        if self.is_null(key_index) {
            return None;
        }

        Some(unsafe { self.value_unchecked(key_index) })
    }
}

pub trait ArrowTypedDictionaryValueIndexAccessor {
    fn get_value_index_null_safe(&self, key_index: usize) -> Option<usize>;
}

impl<K: ArrowDictionaryKeyType, V: Array> ArrowTypedDictionaryValueIndexAccessor
    for TypedDictionaryArray<'_, K, V>
{
    fn get_value_index_null_safe(&self, key_index: usize) -> Option<usize> {
        let keys = self.keys();
        if keys.is_null(key_index) {
            return None;
        }

        let value_index = K::Native::as_usize(unsafe { keys.value_unchecked(key_index) });

        if let Some(value_nulls) = self.values().nulls()
            && value_nulls.is_null(value_index)
        {
            return None;
        }

        Some(value_index)
    }
}

pub trait ArrowGenericByteArrayBufferAccessor {
    unsafe fn get_buffer_value_unchecked(&self, value_index: usize) -> Buffer;
}

impl<T: ByteArrayType> ArrowGenericByteArrayBufferAccessor for GenericByteArray<T> {
    unsafe fn get_buffer_value_unchecked(&self, value_index: usize) -> Buffer {
        let offsets = self.value_offsets();
        let start = T::Offset::as_usize(unsafe { *offsets.get_unchecked(value_index) });
        let end = T::Offset::as_usize(unsafe { *offsets.get_unchecked(value_index + 1) });
        self.values().slice_with_length(start, end - start).clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fill_bit_range_unchecked() {
        let mut buffer = MutableBuffer::from_len_zeroed(8);

        unsafe { fill_bit_range_unchecked(&mut buffer, 0, 1) };

        assert_eq!(&[0x01, 0, 0, 0, 0, 0, 0, 0], buffer.to_byte_slice());

        unsafe { fill_bit_range_unchecked(&mut buffer, 0, 2) };

        assert_eq!(&[0x03, 0, 0, 0, 0, 0, 0, 0], buffer.to_byte_slice());

        unsafe { fill_bit_range_unchecked(&mut buffer, 7, 8) };

        assert_eq!(&[0x83, 0, 0, 0, 0, 0, 0, 0], buffer.to_byte_slice());

        unsafe { fill_bit_range_unchecked(&mut buffer, 8, 17) };

        assert_eq!(&[0x83, 0xFF, 0x01, 0, 0, 0, 0, 0], buffer.to_byte_slice());

        unsafe { fill_bit_range_unchecked(&mut buffer, 25, 26) };

        assert_eq!(
            &[0x83, 0xFF, 0x01, 0x02, 0, 0, 0, 0],
            buffer.to_byte_slice()
        );

        unsafe { fill_bit_range_unchecked(&mut buffer, 33, 35) };

        assert_eq!(
            &[0x83, 0xFF, 0x01, 0x02, 0x06, 0, 0, 0],
            buffer.to_byte_slice()
        );

        unsafe { fill_bit_range_unchecked(&mut buffer, 41, 63) };

        assert_eq!(
            &[0x83, 0xFF, 0x01, 0x02, 0x06, 0xFE, 0xFF, 0x7F],
            buffer.to_byte_slice()
        );
    }
}
