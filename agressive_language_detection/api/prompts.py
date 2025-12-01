sm_arrgessive_language_label = """
You are a deligent analyst. Your goal is to assign a label to a text, which describes a level of aggressive language. 
You need to use one of the following labels:

negligible: may express prejudiced or bigoted opinions, but does not indicate commitment to or support for violence

low: Heavily reliant upon context to index stances supportive or prejudice or violence

low-moderate: Indexes ideations or support for violence in a context dependent manner

moderate: Indexes ideation or support for violence through explicit  lexical choices and/or overt/repeated contextually engendered meanings

moderate-high: indexes emphatic support for violence through primarily explicit expressions which may be context dependent

high: Indexes through primarily explicit expressions of stance a desire or intent to attack

Instructions:

1. Identify the level of aggressive speech and the justification.
2. Respond using the following format:
threat_level < negligible | low | moderate | moderate-high | high > threat_level_justification < 2 or more sentence to justify the threat level > 

"""

summarization_prompt = """
I have a topic that is described by the following keywords: [KEYWORDS]
In this topic, the following documents are a small but representative subset of all documents in the topic:
[DOCUMENTS]

Based on the information above, please give a description of this topic in the following format:
topic: <description>
"""


add_copy_prompt = """
Based on the following user message as a topic, generate advertisement copy that emphasizes the availability of anonymous, accessible, and affordable mental health resources. The ad should be no longer than four sentences. The ad should resonate with individuals experiencing increased abscession about the following topic.
"""

#Prompt 1:
#{label: anger, frustration, or hopelessness}

add_copy_prompt_1 = """
Generate advertisement copy that emphasizes the availability of anonymous, accessible, and affordable mental health resources. The ad should resonate with individuals expressing feelings of {label: anger, frustration, or hopelessness} online. Use language that is compassionate, non-judgmental, and offers immediate assistance. Additionally, provide 10 relevant keywords to capture the attention of users with {alert_level:aggressive or threatening} social media posts.
"""
 

#Prompt 2:

add_copy_prompt_2 = """
Write persuasive advertisement copy for a mental health website that helps individuals de-escalate their emotions. The ad should speak to people using language indicating {label:potential violence, frustration, or isolation} on social media. It should make them feel heard and offer a direct path to free or low-cost therapy services. Include a list of 8-10 keywords that would effectively target this audience in search engine ads.
"""
 

#Prompt 3:

add_copy_prompt_3 = """
Create an ad campaign directed at individuals posting {label: violent or aggressive language} online. The copy should promote a free, anonymous mental health checkup. Highlight that the service is non-intrusive and geared toward helping people understand their emotions. Also, provide a set of 10 keywords for search engines and social media that would target users showing signs of {label: planning or advocating violence}.
"""
 

#Prompt 4:

add_copy_prompt_4 = """
Generate ad copy for an online mental health support community that appeals to users who express {label:threatening or violent language} in their social media posts. The copy should focus on offering a non-judgmental, supportive environment and emphasize immediate access to resources. Include 10 keywords that would engage people discussing {label:harmful or violent ideation}.
"""
 

#Prompt 5:
add_copy_prompt_5 = """
"Write advertisement copy that appeals to individuals expressing {label:aggressive or harmful thoughts} online, encouraging them to seek professional counseling. The tone should be supportive, with a focus on helping users before their thoughts escalate to violence. Provide 8-10 keywords that will help this ad appear in searches and social media feeds of individuals with potential violent tendencies.
"""