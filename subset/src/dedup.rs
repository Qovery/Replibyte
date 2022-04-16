use std::collections::HashSet;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Error, Read, Seek, SeekFrom, Write};
use std::path::Path;

pub type Line<'a> = &'a str;
pub type GroupHash = String;

/// Deduplicate lines from a file.
///
/// ## How it works
/// This function basically take a file path as input and alternates the content of this file to
/// literally deduplicate every matching lines.
/// This function is optimized to not eat too much memory since it can be used on very large file.
/// (However, it has not been benched yet). Here is how it works and how we keep the memory usage as low as possible:
///
/// 1. Find the *portion* of the file where the *matched lines* are (start index, end index).
/// 2. Group lines by pattern on disk or memory
/// 3. Deduplicate groups
/// 4. Rewrite file portion with deduplicated data
pub fn dedup_lines<F: Fn(Line) -> bool, G: FnMut(Line) -> GroupHash>(
    file_path: &Path,
    match_line: F,
    mut group: G,
) -> Result<(), Error> {
    let original_file = File::open(file_path)?;
    let mut reader = BufReader::new(original_file);
    let mut hashes = HashSet::new();
    let temp_directory = tempfile::tempdir()?;
    let temp_directory_path = temp_directory.as_ref();

    let mut first_portion_position = 0u64;
    let mut last_portion_position = 0u64;

    let mut header_buffer = vec![];

    // dedup
    loop {
        let mut line = String::new();
        let res = reader.read_line(&mut line)?;
        if res == 0 {
            // EOF
            break;
        }

        if match_line(line.as_str()) {
            last_portion_position = reader.stream_position()?;
            if first_portion_position == 0 {
                first_portion_position = reader.stream_position()?;
            }

            let hash = group(line.as_str());
            // Potential improvement: in the future we can use a s3 bucket to store the files
            // instead of the local storage, it can help to process big files anywhere.
            // FIXME we can use a bloom filter here to improve the `add_line` performance
            let _ = dedup_line_with_file(temp_directory_path, &hash, line.as_str())?;
            let _ = hashes.insert(hash);
        }

        if first_portion_position == 0 {
            for byte in line.as_bytes() {
                header_buffer.push(*byte);
            }
        }
    }

    // copy footer file to footer buffer
    let mut original_file = File::open(file_path)?;
    let _ = original_file.seek(SeekFrom::Start(last_portion_position))?;
    let mut footer_buffer = vec![];
    let _ = original_file.read_to_end(&mut footer_buffer)?;

    // copy header buffer to file
    let mut original_file = OpenOptions::new()
        .write(true)
        .truncate(true)
        .open(file_path)?;

    let _ = original_file.write_all(header_buffer.as_slice())?;

    // read each hash file and insert contents into original file
    for hash in hashes {
        let hash_file = File::open(temp_directory_path.join(hash))?;
        let hash_reader = BufReader::new(hash_file);
        for hash_line in hash_reader.lines() {
            let hash_line = hash_line?;
            let _ = original_file.write(hash_line.as_bytes())?;
        }
    }

    // cope footer buffer to file
    let _ = original_file.write_all(footer_buffer.as_slice())?;

    Ok(())
}

/// Create or find the appropriate file based on the `group_hash` and append the line if it does not already exist.
fn dedup_line_with_file(
    temp_directory: &Path,
    group_hash: &GroupHash,
    line: Line,
) -> Result<(), Error> {
    let file_path = temp_directory.join(group_hash);
    let file = match File::open(file_path.as_path()) {
        Ok(file) => file,
        Err(_) => File::create(file_path.as_path())?,
    };

    let mut buf = String::new();
    let mut reader = BufReader::new(file);
    while let Ok(amount) = reader.read_line(&mut buf) {
        if amount == 0 {
            // EOF
            break;
        }

        if buf.as_str() == line {
            // the line already exist in the file, we can stop here
            return Ok(());
        }

        let _ = buf.clear();
    }

    // append the line because it does not exist
    let mut file = OpenOptions::new()
        .write(true)
        .append(true)
        .open(file_path.as_path())?;

    let _ = write!(file, "{}", line)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::dedup::dedup_lines;
    use dump_parser::postgres::{
        get_tokens_from_query_str, get_word_value_at_position, trim_pre_whitespaces, Keyword,
    };
    use std::collections::hash_map::DefaultHasher;
    use std::collections::HashSet;
    use std::hash::{Hash, Hasher};
    use std::io::{BufRead, BufReader, Write};

    const DUPLICATED_LINES: &str = r#"
CREATE TABLE public.categories (
    category_id smallint NOT NULL,
    category_name character varying(15) NOT NULL,
    description text,
    picture bytea
);

ALTER TABLE public.categories OWNER TO root;

--
-- Data for Name: categories; Type: TABLE DATA; Schema: public; Owner: root
--

INSERT INTO public.categories (category_id, category_name, description, picture) VALUES (1, 'Beverages', 'Soft drinks, coffees, teas, beers, and ales', '\x');
INSERT INTO public.categories (category_id, category_name, description, picture) VALUES (2, 'Condiments', 'Sweet and savory sauces, relishes, spreads, and seasonings', '\x');
INSERT INTO public.categories (category_id, category_name, description, picture) VALUES (3, 'Confections', 'Desserts, candies, and sweet breads', '\x');
INSERT INTO public.customers (customer_id, company_name, contact_name, contact_title, address, city, region, postal_code, country, phone, fax) VALUES ('AROUT', 'Around the Horn', 'Thomas Hardy', 'Sales Representative', '120 Hanover Sq.', 'London', NULL, 'WA1 1DP', 'UK', '(171) 555-7788', '(171) 555-6750');
INSERT INTO public.customers (customer_id, company_name, contact_name, contact_title, address, city, region, postal_code, country, phone, fax) VALUES ('BERGS', 'Berglunds snabbköp', 'Christina Berglund', 'Order Administrator', 'Berguvsvägen  8', 'Luleå', NULL, 'S-958 22', 'Sweden', '0921-12 34 65', '0921-12 34 67');
INSERT INTO public.categories (category_id, category_name, description, picture) VALUES (4, 'Dairy Products', 'Cheeses', '\x');
INSERT INTO public.categories (category_id, category_name, description, picture) VALUES (4, 'Dairy Products', 'Cheeses', '\x');
INSERT INTO public.categories (category_id, category_name, description, picture) VALUES (4, 'Dairy Products', 'Cheeses', '\x');
INSERT INTO public.categories (category_id, category_name, description, picture) VALUES (5, 'Grains/Cereals', 'Breads, crackers, pasta, and cereal', '\x');
INSERT INTO public.categories (category_id, category_name, description, picture) VALUES (6, 'Meat/Poultry', 'Prepared meats', '\x');
INSERT INTO public.categories (category_id, category_name, description, picture) VALUES (7, 'Produce', 'Dried fruit and bean curd', '\x');
INSERT INTO public.customers (customer_id, company_name, contact_name, contact_title, address, city, region, postal_code, country, phone, fax) VALUES ('BLAUS', 'Blauer See Delikatessen', 'Hanna Moos', 'Sales Representative', 'Forsterstr. 57', 'Mannheim', NULL, '68306', 'Germany', '0621-08460', '0621-08924');
INSERT INTO public.categories (category_id, category_name, description, picture) VALUES (8, 'Seafood', 'Seaweed and fish', '\x');
INSERT INTO public.categories (category_id, category_name, description, picture) VALUES (4, 'Dairy Products', 'Cheeses', '\x');
INSERT INTO public.categories (category_id, category_name, description, picture) VALUES (8, 'Seafood', 'Seaweed and fish', '\x');

--
-- Data for Name: customer_customer_demo; Type: TABLE DATA; Schema: public; Owner: root
--

--
-- Data for Name: customer_demographics; Type: TABLE DATA; Schema: public; Owner: root
--

--
-- Data for Name: customers; Type: TABLE DATA; Schema: public; Owner: root
--

INSERT INTO public.customers (customer_id, company_name, contact_name, contact_title, address, city, region, postal_code, country, phone, fax) VALUES ('ALFKI', 'Alfreds Futterkiste', 'Maria Anders', 'Sales Representative', 'Obere Str. 57', 'Berlin', NULL, '12209', 'Germany', '030-0074321', '030-0076545');
INSERT INTO public.customers (customer_id, company_name, contact_name, contact_title, address, city, region, postal_code, country, phone, fax) VALUES ('ANATR', 'Ana Trujillo Emparedados y helados', 'Ana Trujillo', 'Owner', 'Avda. de la Constitución 2222', 'México D.F.', NULL, '05021', 'Mexico', '(5) 555-4729', '(5) 555-3745');
INSERT INTO public.customers (customer_id, company_name, contact_name, contact_title, address, city, region, postal_code, country, phone, fax) VALUES ('ANATR', 'Ana Trujillo Emparedados y helados', 'Ana Trujillo', 'Owner', 'Avda. de la Constitución 2222', 'México D.F.', NULL, '05021', 'Mexico', '(5) 555-4729', '(5) 555-3745');
INSERT INTO public.categories (category_id, category_name, description, picture) VALUES (8, 'Seafood', 'Seaweed and fish', '\x');
INSERT INTO public.customers (customer_id, company_name, contact_name, contact_title, address, city, region, postal_code, country, phone, fax) VALUES ('ANATR', 'Ana Trujillo Emparedados y helados', 'Ana Trujillo', 'Owner', 'Avda. de la Constitución 2222', 'México D.F.', NULL, '05021', 'Mexico', '(5) 555-4729', '(5) 555-3745');
INSERT INTO public.customers (customer_id, company_name, contact_name, contact_title, address, city, region, postal_code, country, phone, fax) VALUES ('ANTON', 'Antonio Moreno Taquería', 'Antonio Moreno', 'Owner', 'Mataderos  2312', 'México D.F.', NULL, '05023', 'Mexico', '(5) 555-3932', NULL);
INSERT INTO public.customers (customer_id, company_name, contact_name, contact_title, address, city, region, postal_code, country, phone, fax) VALUES ('BLAUS', 'Blauer See Delikatessen', 'Hanna Moos', 'Sales Representative', 'Forsterstr. 57', 'Mannheim', NULL, '68306', 'Germany', '0621-08460', '0621-08924');
INSERT INTO public.customers (customer_id, company_name, contact_name, contact_title, address, city, region, postal_code, country, phone, fax) VALUES ('BLAUS', 'Blauer See Delikatessen', 'Hanna Moos', 'Sales Representative', 'Forsterstr. 57', 'Mannheim', NULL, '68306', 'Germany', '0621-08460', '0621-08924');
INSERT INTO public.customers (customer_id, company_name, contact_name, contact_title, address, city, region, postal_code, country, phone, fax) VALUES ('BLONP', 'Blondesddsl père et fils', 'Frédérique Citeaux', 'Marketing Manager', '24, place Kléber', 'Strasbourg', NULL, '67000', 'France', '88.60.15.31', '88.60.15.32');
INSERT INTO public.categories (category_id, category_name, description, picture) VALUES (5, 'Grains/Cereals', 'Breads, crackers, pasta, and cereal', '\x');
INSERT INTO public.customers (customer_id, company_name, contact_name, contact_title, address, city, region, postal_code, country, phone, fax) VALUES ('BOLID', 'Bólido Comidas preparadas', 'Martín Sommer', 'Owner', 'C/ Araquil, 67', 'Madrid', NULL, '28023', 'Spain', '(91) 555 22 82', '(91) 555 91 99');
INSERT INTO public.categories (category_id, category_name, description, picture) VALUES (6, 'Meat/Poultry', 'Prepared meats', '\x');
INSERT INTO public.customers (customer_id, company_name, contact_name, contact_title, address, city, region, postal_code, country, phone, fax) VALUES ('AROUT', 'Around the Horn', 'Thomas Hardy', 'Sales Representative', '120 Hanover Sq.', 'London', NULL, 'WA1 1DP', 'UK', '(171) 555-7788', '(171) 555-6750');
INSERT INTO public.customers (customer_id, company_name, contact_name, contact_title, address, city, region, postal_code, country, phone, fax) VALUES ('BONAP', 'Bon app''', 'Laurence Lebihan', 'Owner', '12, rue des Bouchers', 'Marseille', NULL, '13008', 'France', '91.24.45.40', '91.24.45.41');

--
-- Name: employees fk_employees_employees; Type: FK CONSTRAINT; Schema: public; Owner: root
--

ALTER TABLE ONLY public.employees
    ADD CONSTRAINT fk_employees_employees FOREIGN KEY (reports_to) REFERENCES public.employees(employee_id);


--
-- Name: order_details fk_order_details_orders; Type: FK CONSTRAINT; Schema: public; Owner: root
--

ALTER TABLE ONLY public.order_details
    ADD CONSTRAINT fk_order_details_orders FOREIGN KEY (order_id) REFERENCES public.orders(order_id);

        "#;

    #[test]
    fn check_dedup_file() {
        let named_temp_file = tempfile::NamedTempFile::new().unwrap();
        let mut file = named_temp_file.as_file();
        let _ = file.write_all(DUPLICATED_LINES.as_bytes()).unwrap();

        let _ = dedup_lines(
            named_temp_file.as_ref(),
            |line| line.contains("INSERT INTO"),
            |line| {
                let tokens = get_tokens_from_query_str(line);
                let tokens = trim_pre_whitespaces(tokens);
                let database = get_word_value_at_position(&tokens, 4).unwrap();
                let table = get_word_value_at_position(&tokens, 6).unwrap();
                let key = format!("{}-{}", database, table);
                key
            },
        )
        .unwrap();

        let reader = BufReader::new(file);
        let mut hash_set = HashSet::new();

        for line in reader.lines() {
            let line = line.unwrap();
            if hash_set.contains(line.as_str()) {
                assert!(false);
            }

            hash_set.insert(line);
        }
    }
}
