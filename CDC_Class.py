import requests


class CDC_URL:
    def __init__(self, key):
        assert isinstance(key, str)
        assert len(key) == 9
        assert key[4] == '-'
        self.url = f'https://data.cdc.gov/resource/{key}.json'
        data = requests.get(f'{self.url}?$select=count(state)').json()
        self.num_rows = int(data[0]['count_state'])
        self.url_list = self.get_urls()

    def get_urls(self):
        if self.num_rows < 1000:
            return self.url

        elif self.num_rows >= 1000 and self.num_rows < 10000:
            return f'{self.url}?$limit={self.num_rows}'

        elif self.num_rows >= 10000 and self.num_rows < 50000:
            parse_list = []
            for i in range(1, 6):
                if self.num_rows / i < 10000:  # see which number of parts results in less than 10,000 rows
                    parse_list.append(self.num_rows // i)  # take the floor division result and add it to parse list
            parse = max(parse_list)  # take the maximum of this list because I want the fewest files... less time
            # print(parse)

            offset = []
            position = 0
            while position < self.num_rows:
                offset.append(position)
                position += (parse)

            url_list = []
            for pos in offset:
                url_list.append(f'{self.url}?$limit={parse}&$offset={pos}')

            return url_list


        elif self.num_rows >= 50000 and self.num_rows < 200000:
            parse_list = []
            for i in range(9, 20):
                if num_rows / i <= 20000:  # see which number of parts results in less than 20,000 rows
                    parse_list.append(self.num_rows // i)  # take the floor division result and add it to parse list
            parse = max(parse_list)  # take the minimum from this list because I want to get fewest file parses

            offset = []
            position = 0
            while position < self.num_rows:
                offset.append(position)
                position += (parse)

            url_list = []
            for pos in offset:
                url_list.append(f'{self.url}?$limit={parse}&$offset={pos}')

            return url_list
        elif self.num_rows > 200000:
            raise Exception('Row limit exceeded!')


    def __len__(self):
        return len(self.url_list)

    def __str__(self):
        return f'This URL has a total of {len(self)} file(s) that will be uploaded.'


