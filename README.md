# NachoDB 🧀
A simple sqlite-like database written in C

---

# Why, though?

To practice C after a 8 year hiatus, I'm building small projects in the language. It's a rough and dirty data store but serves to improve my grasp on C. 
<br />
<br />
Here's the NachoDB prompt:

```
nacho > 
```

NachoDB has a single table with a fixed schema:

| id  | username     | email        |
|-----|--------------|--------------|
| int | varchar(255) | varchar(255) |

The records look like this:

| ID | Name   | Email            |
|----|--------|------------------|
| 0  | John   | jdoe@gmail.com   |
| 1  | Dave   | dsmith@gmail.com |
| 2  | Jerry  | jwang@gmail.com  |

<br />

Here are some useful commands:

1. `insert`
2. `select`
3. `.exit`

<br />

To see the bytes being stored on the disk, go to the `src` directory and type the following in terminal:

```bash
vim rishtest.nacho
:%!xxd
```

This allows us to see the individual bytes and the corresponding data.

---

# License

MIT