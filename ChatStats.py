# -*- coding: utf-8 -*-

from telethon.sync import TelegramClient
from telethon.tl.types import PeerChannel
from datetime import datetime, timedelta, date
import datetime
import pandas as pd

import re
import emoji

from pymorphy2 import MorphAnalyzer
import nltk
from nltk import word_tokenize
from nltk.probability import FreqDist
from nltk.corpus import stopwords
from sqlalchemy import create_engine

pd.options.display.max_colwidth = 150
pd.set_option('chained_assignment', None)

# id и hash с сайта https://my.telegram.org/apps
api_id =
api_hash = ''
phone = ''
client = TelegramClient(phone, api_id, api_hash)

# авторизация (пароль, если 2FA)
client.start(password='')

# запрос информации о канале (группе)
chat_title = client.get_entity(PeerChannel())

def get_statistics():
    # запрос сообщений за последние два дня
    messages = client.get_messages(
        entity=PeerChannel(channel_id=),  # limit=?
        offset_date=datetime.datetime.now()
                    .replace(hour=0, minute=0, second=0, microsecond=0) -
                    datetime.timedelta(hours=51), reverse=True
    )

    # заготовка для датафрейма (дф)
    df = pd.DataFrame(
        columns=['message_id', 'date', 'from_id', 'from_name', 'from_username', 'message', 'reactions_count'])

    # формирование дф
    for i in range(len(messages)):
        if not messages[i].fwd_from and type(messages[i]) != 'telethon.tl.patched.MessageService':

            # формирование юзернейма и хэндлера пользователя
            if messages[i].from_id:
                # для сообщений от пользователей
                from_id = client.get_entity(messages[i].from_id)
                handler = '@' + str(from_id.username)
                name = from_id.first_name + ' ' + from_id.last_name if from_id.last_name else from_id.first_name
                user_id = messages[i].from_id.user_id
            else:
                # для сообщений админов от имени группы
                handler, user_id = '--', '--'
                name = client.get_entity(messages[i].peer_id).title

            # добавляем ко времени публикации сообщения 3 часа, чтобы оно соответствовало московскому времени
            actual_date = messages[i].date + datetime.timedelta(hours=3)

            # создание списка с информацией о сообщении, который будет добавляться новой стройкой в дф
            msg_info = [messages[i].id, actual_date, user_id, name, handler, messages[i].message]

            # проверка на наличие реакций
            if messages[i].reactions:
                reactions_count = 0

                # подсчет количества реакций
                for cnt in messages[i].reactions.results:
                    reactions_count += cnt.count

                # добавление в список
                msg_info.append(reactions_count)
            else:
                msg_info.append(0)

            # добавление в дф новой строки с информацией о сообщении
            df.loc[i] = msg_info

    # Приводим столбец со временем публикации сообщения к формату date.
    df['date'] = df['date'].apply(lambda x: datetime.date(x.year, x.month, x.day))

    # Фильтруем сообщения по дате: за вчера и за позавчера.
    yest = date.today() - timedelta(days=1)
    two_days_before = date.today() - timedelta(days=2)
    yest_fmt = yest.strftime(format='%d-%m-%Y')

    messages_yest = df[df['date'] == yest]
    messages_2_days_before = df[df['date'] == two_days_before]

    # Подготовка функций

    # Для нахождения сообщений с благодарностью

    def top_thankful(row):
        regex = r'пасиб|спс|thank|благодар(?=ю|им|ен|на)'
        if re.findall(regex, str(row), flags=re.IGNORECASE):
            result = 1
        else:
            result = 0
        return result

    # Для нахождения сообщений с вопросами

    def top_inquisitive(row):
        regex = r'(?<=[ \w])\?\B'
        if re.findall(regex, str(row)):
            result = 1
        else:
            result = 0
        return result

    # Для нахождения сообщений с матом

    def top_swearers(row):
        regex = r"\b(?<!-)(?:(?:[ао]|сни|[дн][иоа][ -]?|п[ао])?[хзкб]у-?[ й#*@]?(?:ца|[иею][вцн]?|ет[уаь]|н[яюе]|е?л[иье](?:арда?)?\b|я(?: [тс]ебе)?(?!н)| [твн]ам| [тс]ебе))(?!ть)|(?:п[иеё]*[сз\*][длж])|(?<![лртншчд])(?:[её][б\*]л?[ао]?[нл](?:ат|ько|))|(?<![вдрнчлтшц])(?:[её][б\*](?:[уеас]*ть?|н?у(?:ч|чк.)?|[ао]ш|цч|и|(?: тв| ва|ы)|е[йн]))|\b(?:по|на|с)?[хз]у[яй]|(?:су+к(?:[аи]|ой|аэл(?:ь|ем?)(?:чик)?|ами?)?)\b|(?<![рауеом])(?:ука)?(?:бл[я*эе][тд]?(?:ью?|я*|ей|ями?|ск)?)(?![дмнксвфй])|(?<!у)(?:муда?[^р ])|н[еи] ип(?:[уе]т?)|(?<!ски)пид[аое]р|(?:долб[ао]|[уьъ]|[нз]а)[её][бп]|(?<!р)суч(?:ар|ь|ий)|збс"
        findall = re.findall(regex, str(row), flags=re.IGNORECASE)
        result = len(findall) if findall else 0
        return result

    # Для подсчета длины сообщений (очищаем от эмоджи и разбиваем на слова)

    def words(row):
        de_emoj = emoji.demojize(str(row))
        regex = r":[a-z_]*:|{?'type'(?:\: ?)*|{?'code'(?:\: ?)*|{?'text'(?:\: ?)*|'|{|}"
        clean_text = re.sub(regex, '', de_emoj)
        strip = clean_text.strip()
        regex = r'\b[а-яё-]+\b'
        words_count = re.findall(regex, strip, flags=re.IGNORECASE)
        return len(words_count)

    messages_yest['thanks'] = messages_yest['message'].apply(top_thankful)
    messages_yest['questions'] = messages_yest['message'].apply(top_inquisitive)
    messages_yest['swearer'] = messages_yest['message'].apply(top_swearers)
    messages_yest['symbols'] = messages_yest['message'].apply(lambda x: len(str(x)))
    messages_yest['words'] = messages_yest['message'].apply(words)

    # Расчет статистических данных

    # Топ самых разговорчивых участников

    msgs_grp = (
        messages_yest[messages_yest['from_id'] != ]
        .groupby(['from_name', 'from_id', 'from_username'])
        ['message'].count().sort_values(ascending=False).reset_index().head(10)
    )

    # Топ самых благодарных

    thanks_grp = (
        messages_yest[(messages_yest['thanks'] == 1) & (messages_yest['from_name'] != 'Tiny🍀Ton') &
                      (messages_yest['from_id'] != )]
        .groupby(['from_name', 'from_id', 'from_username'])['thanks']
        .count().sort_values(ascending=False).reset_index().head(5)
    )

    # Топ самых любознательных

    questions_grp = (
        messages_yest[(messages_yest['questions'] == 1) & (messages_yest['from_id'] != )]
        .groupby(['from_name', 'from_id', 'from_username'])['questions']
        .count().sort_values(ascending=False).reset_index().head(3)
    )

    # Топ самых молчаливых из тех, кто отправил хотя бы одно сообщение за день

    most_reticent = (
        messages_yest[messages_yest['from_id'] != ]
        .groupby(['from_name', 'from_id', 'from_username'])
        ['message'].count().sort_values().reset_index().head(5)
    )

    # Топ матерщинников

    swearers_grp = (
        messages_yest[(messages_yest['swearer'] > 0) &
                      (messages_yest['from_id'] != ) &
                      (messages_yest['from_name'] != 'Tiny🍀Ton')]
        .groupby(['from_name', 'from_id', 'from_username'])['swearer']
        .sum().sort_values(ascending=False).reset_index().head(5)
    )

    # Нахождение самого длинного сообщения

    longest_post = (
        messages_yest[(messages_yest['from_name'] != 'Tiny🍀Ton') &
                      (messages_yest['from_id'] != )]
        [['from_name', 'from_id', 'from_username', 'words', 'message_id']]
        .sort_values(by='words', ascending=False).head(3)
    )

    # Нахождение самого "эмоционального" сообщения

    most_engaging = (
        messages_yest[(messages_yest['from_id'] != ) &
                      ~(messages_yest['message'].apply(lambda x: True if any(st in str(x)
                                                                             for st in ['"комната источающая свет"',
                                                                                        'Погода на сегодня, ']) else False))]
        [['from_name', 'from_id', 'from_username', 'reactions_count', 'message_id']]
        .sort_values(by='reactions_count', ascending=False).head(3)
    )

    # Расчет абсолютной и относительной разницы в количестве общавшихся за два дня

    unique_yest = messages_yest['from_id'].unique()
    unique_2_days_before = messages_2_days_before['from_id'].unique()
    diff_chatting = len(unique_yest) - len(unique_2_days_before)
    diff = round((len(messages_yest) - len(messages_2_days_before)) / len(messages_2_days_before) * 100, 1)

    # Расчет медианного значения количества слов и символов

    mdn_words = int(messages_yest['words'].median())
    mdn_symbols = int(messages_yest['symbols'].median())

    # Определение формулировок для итогового сообщения

    if diff_chatting == 0:
        chatting = 'столько же, сколько и днем ранее'
    elif diff_chatting > 0:
        chatting = f'на {diff_chatting} больше, чем днем ранее'
    else:
        chatting = f'на {abs(diff_chatting)} меньше, чем днем ранее'

    people = 'человека' if len(unique_yest) % 10 in (2, 3, 4) else 'человек'

    wrd = longest_post.iloc[0][3]
    rct = most_engaging.iloc[0][3]

    def wordform(cnt):
        if cnt % 100 in (11, 12, 13, 14):
            wrdform, reactions = 'слов', 'реакций'
        else:
            if cnt % 10 == 1:
                wrdform, reactions = 'слово', 'реакция'
            elif cnt % 10 in (2, 3, 4):
                wrdform, reactions = 'слова', 'реакции'
            else:
                wrdform, reactions = 'слов', 'реакций'

        result = wrdform if cnt == wrd else reactions

        return result

    msgs = 'меньше' if diff < 0 else 'больше'

    # Расчет топ-10 самых частых слов

    # Функция для очищения текста от эмоджи и знаков препинания

    def full_clean_text(row):
        de_emoj = emoji.demojize(str(row))
        regex = r"""[—'{}\\:;,.()\[\]a-z@\/?\+!0-9&_=#$%^*~\"…«»-]+|\n"""
        clean_text = re.sub(regex, ' ', de_emoj, flags=re.IGNORECASE)
        return clean_text

    # Лемматизация слов

    messages_yest_wo_me = messages_yest[messages_yest['from_name'] != 'Tiny🍀Ton']
    messages_yest_wo_me['clean_text'] = messages_yest_wo_me['message'].apply(full_clean_text)
    morph = MorphAnalyzer()
    clean_text = []
    for message in messages_yest_wo_me['clean_text']:
        token = list(map(lambda x: morph.normal_forms(x.strip())[0], message.lower().split()))
        clean_text.extend(token)

    clean_text = ' '.join(clean_text)
    clean_text = clean_text.replace('ё', 'е')

    text_tokens = word_tokenize(clean_text)
    text_tokens = nltk.Text(text_tokens)

    # Исключение из топа стоп-слов, не несущих смысл для понимания тематики общения в чате

    russian_stopwords = stopwords.words("russian")
    russian_stopwords.extend(['это', 'еще', 'очень', 'вообще', 'просто', 'кстати', 'вроде',
                              'хз', 'тебе', 'ок', 'пока', 'типа', 'какие', 'кажется',
                              'сегодня', 'завтра', 'поэтому', 'такой', 'вчера', 'норм',
                              'спасибо', 'блин', 'ага', 'ща', 'прям', 'который', 'весь',
                              'свой', 'тип', 'мой', 'твой', 'наш', 'ваш', 'самый', 'ой',
                              'мочь', 'почему', 'р'])

    words_frequency_wo_sw = [word for word in text_tokens if word not in russian_stopwords]
    words_frequency_wo_sw = FreqDist(words_frequency_wo_sw)
    top_10_words = words_frequency_wo_sw.most_common(10)

    top_10_words_str = ''
    for i in range(len(top_10_words)):
        top_10_words_str += top_10_words[i][0] + ', '
    # top_10_words_str[:-2]

    # Подсчет количества слов с корнем пид(о/а)р
    word_list = words_frequency_wo_sw.most_common()
    pidor = 0
    for k, v in word_list:
        if k.count('пидор') or k.count('пидар') or k.count('пидр'):
            pidor += v

    # Записываем получившееся число в базу данных для подсчета недельного результата

    engine = create_engine('sqlite:///C:/SQLite/dbs/tg_chat.db')
    engine.connect()

    query = f'''DELETE FROM pidors
                       WHERE dt = '{yest}'
             '''
    engine.execute(query)

    pidors_cnt = pd.DataFrame([[yest, pidor]],
                              columns=['dt', 'cnt'])

    pidors_cnt.to_sql(name='pidors', con=engine, if_exists='append', index=False)

    # Формирование сообщения с отчетом

    text = f"""❗️❗️❗️\n\n**"{chat_title.title}"**\n\nСтатистика чата за вчера ({yest_fmt}).\n\n"""
    text += f"""Оставлено сообщений: {len(messages_yest)},\nчто **на {str(abs(diff)).replace('.', ',')}% {msgs}**, чем днем ранее.\n\n"""
    text += f"""Всего общались: **{len(unique_yest)} {people}**\n({chatting}).\n\n**Медианная длина** сообщения:\n"""
    text += f"""слов — {mdn_words}, символов — {mdn_symbols}\n\nТоп-10 **самых частых** слов:\n{top_10_words_str[:-2]}\n\n"""
    text += f"""__Самые разговорчивые в чате__:\n"""
    text += f"""1) **{msgs_grp.iloc[0][0]}** ({msgs_grp.iloc[0][2]}) (сообщений: {msgs_grp.iloc[0][3]}) 🥇\n"""
    text += f"""2) **{msgs_grp.iloc[1][0]}** ({msgs_grp.iloc[1][2]}) (сообщений: {msgs_grp.iloc[1][3]}) 🥈\n"""
    text += f"""3) **{msgs_grp.iloc[2][0]}** ({msgs_grp.iloc[2][2]}) (сообщений: {msgs_grp.iloc[2][3]}) 🥉\n"""
    text += f"""4) **{msgs_grp.iloc[3][0]}** ({msgs_grp.iloc[3][2]}) (сообщений: {msgs_grp.iloc[3][3]})\n"""
    text += f"""5) **{msgs_grp.iloc[4][0]}** ({msgs_grp.iloc[4][2]}) (сообщений: {msgs_grp.iloc[4][3]})\n\n"""
    text += f"""__Самое длинное сообщение__:\n"""
    text += f"""**{longest_post.iloc[0][0]}** ({longest_post.iloc[0][2]}) — {longest_post.iloc[0][3]} {wordform(wrd)} (t.me/c/1403290431/{longest_post.iloc[0][4]})\n\n"""
    text += f"""__Самое "эмоциональное" сообщение__:\n"""
    text += f"""**{most_engaging.iloc[0][0]}** ({most_engaging.iloc[0][2]}) — {most_engaging.iloc[0][3]} {wordform(rct)} (t.me/c/1403290431/{most_engaging.iloc[0][4]})\n\n"""
    text += f"""__Самые любознательные__:\n"""

    for i in range(3):
        text += f'{i + 1}) **{questions_grp.iloc[i][0]}** ({questions_grp.iloc[i][2]}) (вопросов: {questions_grp.iloc[i][3]})\n'

    text += '\n__Самые благодарные__:\n'

    try:
        for i in range(3):
            text += f'{i + 1}) **{thanks_grp.iloc[i][0]}** ({thanks_grp.iloc[i][2]}) (поблагодарил(а) раз: {thanks_grp.iloc[i][3]})\n'
    except IndexError:
        pass

    text += '\n__Топ матерщинников__:\n'

    if len(swearers_grp):
        try:
            for i in range(3):
                text += f'{i + 1}) **{swearers_grp.iloc[i][0]}** ({swearers_grp.iloc[i][2]}) (выругался(-ась) раз: {swearers_grp.iloc[i][3]})\n'
        except IndexError:
            pass
    else:
        text += 'Вчера никто не ругался! Дожили!\n'

    text += '\n__Самые неразговорчивые из общавшихся__:\n'

    for i in range(5):
        text += f'{i + 1}) **{most_reticent.iloc[i][0]}** ({most_reticent.iloc[i][2]}) (сообщений: {most_reticent.iloc[i][3]})\n'

    text += f"\n__Специальный пидорский счетчик (чтобы кое-кто хорошо спала)__:\n**Пидоров за день** — {pidor}"""

    # планируем публикацию отчета на 12:00, если сам отчет сформирован до этого времени, и моментально, если сформирован после 12

    if datetime.datetime.now() < datetime.datetime.now().replace(hour=12, minute=0, second=0, microsecond=0):
        client.send_message(entity=PeerChannel(channel_id=), message=text,
                            schedule=datetime.datetime.now().replace(hour=12, minute=0, second=0, microsecond=0) -
                                     datetime.datetime.now())
    else:
        client.send_message(entity=PeerChannel(channel_id=), message=text)


if __name__ == '__main__':
    # main()
    get_statistics()
