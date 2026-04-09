use crate::storage::table::types::Idx;

pub type BitpackingWidth = u8;

pub struct BitpackingPrimitives;

impl BitpackingPrimitives {
    pub const BITPACKING_ALGORITHM_GROUP_SIZE: usize = 32;
    pub const BITPACKING_HEADER_SIZE: usize = std::mem::size_of::<u64>();

    pub fn get_required_size(count: Idx, width: BitpackingWidth) -> usize {
        let count = Self::round_up_to_algorithm_group_size(count as usize);
        (count * width as usize) / 8
    }

    pub fn round_up_to_algorithm_group_size(count: usize) -> usize {
        let remainder = count % Self::BITPACKING_ALGORITHM_GROUP_SIZE;
        if remainder == 0 {
            count
        } else {
            count + Self::BITPACKING_ALGORITHM_GROUP_SIZE - remainder
        }
    }

    pub fn unpack_block<T: BitpackingValue>(
        dst: &mut [T; Self::BITPACKING_ALGORITHM_GROUP_SIZE],
        src: &[u8],
        width: BitpackingWidth,
        skip_sign_extension: bool,
    ) {
        if width == 0 {
            for value in dst.iter_mut() {
                *value = T::default();
            }
            return;
        }
        for (idx, value) in dst.iter_mut().enumerate() {
            let raw = extract_bits(src, idx * width as usize, width as usize);
            *value = if T::SIGNED && !skip_sign_extension && width < T::BITS as u8 {
                sign_extend::<T>(raw, width)
            } else {
                T::from_u128(raw)
            };
        }
    }

    pub fn unpack_buffer<T: BitpackingValue>(
        dst: &mut [T],
        src: &[u8],
        count: usize,
        width: BitpackingWidth,
        skip_sign_extension: bool,
    ) {
        let mut offset = 0usize;
        while offset < count {
            let mut block = [T::default(); Self::BITPACKING_ALGORITHM_GROUP_SIZE];
            Self::unpack_block(
                &mut block,
                &src[(offset * width as usize) / 8..],
                width,
                skip_sign_extension,
            );
            let to_copy = (count - offset).min(Self::BITPACKING_ALGORITHM_GROUP_SIZE);
            dst[offset..offset + to_copy].copy_from_slice(&block[..to_copy]);
            offset += Self::BITPACKING_ALGORITHM_GROUP_SIZE;
        }
    }

    pub fn pack_buffer<T: BitpackingValue>(dst: &mut [u8], src: &[T], count: usize, width: BitpackingWidth) {
        if width == 0 {
            return;
        }
        let rounded = Self::round_up_to_algorithm_group_size(count);
        let required = (rounded * width as usize) / 8;
        dst[..required].fill(0);
        for (idx, value) in src.iter().take(count).enumerate() {
            let raw = if T::SIGNED {
                let mask = if width as usize >= 128 {
                    u128::MAX
                } else {
                    (1u128 << width as usize) - 1
                };
                (value.to_i128() as u128) & mask
            } else {
                value.to_i128() as u128
            };
            insert_bits(dst, idx * width as usize, width as usize, raw);
        }
    }
}

fn mask(width: usize) -> u128 {
    if width == 0 {
        0
    } else if width >= 128 {
        u128::MAX
    } else {
        (1u128 << width) - 1
    }
}

fn extract_bits(src: &[u8], start_bit: usize, width: usize) -> u128 {
    let byte_offset = start_bit / 8;
    let bit_offset = start_bit % 8;
    let byte_count = ((bit_offset + width) + 7) / 8;
    let mut chunk = 0u128;
    for i in 0..byte_count.min(16) {
        let byte = src.get(byte_offset + i).copied().unwrap_or_default() as u128;
        chunk |= byte << (8 * i);
    }
    (chunk >> bit_offset) & mask(width)
}

fn sign_extend<T: BitpackingValue>(raw: u128, width: BitpackingWidth) -> T {
    let sign_bit = 1u128 << (width as usize - 1);
    let extended = ((raw ^ sign_bit) as i128) - sign_bit as i128;
    T::from_i128(extended)
}

fn insert_bits(dst: &mut [u8], start_bit: usize, width: usize, value: u128) {
    if width == 0 {
        return;
    }
    let byte_offset = start_bit / 8;
    let bit_offset = start_bit % 8;
    let byte_count = (bit_offset + width).div_ceil(8);
    let mut chunk = 0u128;
    for i in 0..byte_count.min(16) {
        chunk |= (dst.get(byte_offset + i).copied().unwrap_or_default() as u128) << (8 * i);
    }
    let clear_mask = !(mask(width) << bit_offset);
    chunk = (chunk & clear_mask) | ((value & mask(width)) << bit_offset);
    for i in 0..byte_count.min(16) {
        if let Some(slot) = dst.get_mut(byte_offset + i) {
            *slot = ((chunk >> (8 * i)) & 0xFF) as u8;
        }
    }
}

pub trait BitpackingValue: Copy + Default {
    const BITS: usize;
    const SIGNED: bool;
    const MIN_I128: i128;
    const MAX_I128: i128;

    fn from_u128(value: u128) -> Self;
    fn from_i128(value: i128) -> Self;
    fn to_i128(self) -> i128;
    fn write_le(self, dst: &mut [u8]);
    fn read_le(src: &[u8]) -> Self;
    fn wrapping_add(self, other: Self) -> Self;
    fn wrapping_mul(self, other: Self) -> Self;
    fn checked_sub(self, other: Self) -> Option<Self>;
}

macro_rules! impl_bitpacking_value_signed {
    ($t:ty, $bytes:expr) => {
        impl BitpackingValue for $t {
            const BITS: usize = <$t>::BITS as usize;
            const SIGNED: bool = true;
            const MIN_I128: i128 = <$t>::MIN as i128;
            const MAX_I128: i128 = <$t>::MAX as i128;

            fn from_u128(value: u128) -> Self {
                value as $t
            }

            fn from_i128(value: i128) -> Self {
                value as $t
            }

            fn to_i128(self) -> i128 {
                self as i128
            }

            fn write_le(self, dst: &mut [u8]) {
                dst[..$bytes].copy_from_slice(&self.to_le_bytes());
            }

            fn read_le(src: &[u8]) -> Self {
                let mut bytes = [0u8; $bytes];
                bytes.copy_from_slice(&src[..$bytes]);
                <$t>::from_le_bytes(bytes)
            }

            fn wrapping_add(self, other: Self) -> Self {
                <$t>::wrapping_add(self, other)
            }

            fn wrapping_mul(self, other: Self) -> Self {
                <$t>::wrapping_mul(self, other)
            }

            fn checked_sub(self, other: Self) -> Option<Self> {
                <$t>::checked_sub(self, other)
            }
        }
    };
}

macro_rules! impl_bitpacking_value_unsigned {
    ($t:ty, $bytes:expr) => {
        impl BitpackingValue for $t {
            const BITS: usize = <$t>::BITS as usize;
            const SIGNED: bool = false;
            const MIN_I128: i128 = 0;
            const MAX_I128: i128 = <$t>::MAX as i128;

            fn from_u128(value: u128) -> Self {
                value as $t
            }

            fn from_i128(value: i128) -> Self {
                value as $t
            }

            fn to_i128(self) -> i128 {
                self as i128
            }

            fn write_le(self, dst: &mut [u8]) {
                dst[..$bytes].copy_from_slice(&self.to_le_bytes());
            }

            fn read_le(src: &[u8]) -> Self {
                let mut bytes = [0u8; $bytes];
                bytes.copy_from_slice(&src[..$bytes]);
                <$t>::from_le_bytes(bytes)
            }

            fn wrapping_add(self, other: Self) -> Self {
                <$t>::wrapping_add(self, other)
            }

            fn wrapping_mul(self, other: Self) -> Self {
                <$t>::wrapping_mul(self, other)
            }

            fn checked_sub(self, other: Self) -> Option<Self> {
                <$t>::checked_sub(self, other)
            }
        }
    };
}

impl_bitpacking_value_signed!(i8, 1);
impl_bitpacking_value_signed!(i16, 2);
impl_bitpacking_value_signed!(i32, 4);
impl_bitpacking_value_signed!(i64, 8);
impl_bitpacking_value_unsigned!(u8, 1);
impl_bitpacking_value_unsigned!(u16, 2);
impl_bitpacking_value_unsigned!(u32, 4);
impl_bitpacking_value_unsigned!(u64, 8);
