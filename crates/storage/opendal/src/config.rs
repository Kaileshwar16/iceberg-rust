// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::collections::HashMap;
use std::time::Duration;

use iceberg::io::CLIENT_IO_TIMEOUT_MS;
use iceberg::{Error, ErrorKind, Result};

pub(crate) fn io_timeout_from_props(props: &HashMap<String, String>) -> Result<Option<Duration>> {
    let Some(value) = props.get(CLIENT_IO_TIMEOUT_MS) else {
        return Ok(None);
    };
    let millis = value
        .parse::<u64>()
        .ok()
        .filter(|millis| *millis > 0)
        .ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Invalid {CLIENT_IO_TIMEOUT_MS}: {value}. Expected a positive integer number of milliseconds"
                ),
            )
        })?;
    Ok(Some(Duration::from_millis(millis)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_io_timeout_property() {
        assert_eq!(io_timeout_from_props(&HashMap::new()).unwrap(), None);
        for (value, millis) in [("1", 1), ("10000", 10000), ("120000", 120000)] {
            let props = HashMap::from([(CLIENT_IO_TIMEOUT_MS.to_string(), value.to_string())]);
            assert_eq!(
                io_timeout_from_props(&props).unwrap(),
                Some(Duration::from_millis(millis))
            );
        }
        for value in [
            "",
            "0",
            "-1",
            "1.5",
            "10s",
            "abc",
            " 1000",
            "18446744073709551616",
        ] {
            let props = HashMap::from([(CLIENT_IO_TIMEOUT_MS.to_string(), value.to_string())]);
            let error = io_timeout_from_props(&props).unwrap_err();
            assert_eq!(error.kind(), ErrorKind::DataInvalid);
            assert!(error.message().contains(CLIENT_IO_TIMEOUT_MS));
        }
    }
}
